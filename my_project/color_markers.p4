// Requires v1model version 20200408 or later
#define V1MODEL_VERSION 20200408
#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_IPV4 = 0x800;

// Burst limit τ for the leaky bucket: 4 maximum-sized Ethernet frames (4 × 1514 B × 8 bits).
// A packet is marked green if the virtual water level does not exceed this threshold
// after adding the packet's bit length; otherwise it is marked yellow.
const int<48> tau = 4 * 1514 * 8;

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;

header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    ip4Addr_t srcAddr;
    ip4Addr_t dstAddr;
}

// User-defined metadata passed from the Ingress Pipeline to the Traffic Manager.
// These two fields form the handoff contract between the Color Marker Bank and
// the FIFO Queue Bank / HRDS implemented in QueueingLogicVN:
//   - vn_id  is read via SSWITCH_VN_QUEUEING_SRC to select the per-VN FIFO queue.
//   - color  is read via SSWITCH_COLOR_SRC to decide whether to accumulate the
//            Guaranteed Byte Credit (GB) counter for this packet.
struct metadata {
    bit<8> vn_id;  // VN identifier assigned by vn_classifier; indexes into per-VN queues.
    bit<2> color;  // Conformance mark: 0 = green (within guaranteed bandwidth),
                   //                   1 = yellow (exceeds guaranteed bandwidth).
}

struct headers {
    ethernet_t   ethernet;
    ipv4_t       ipv4;
}

/*************************************************************************
*********************** P A R S E R  *************************************
*************************************************************************/

// Parse Ethernet and IPv4 headers in sequence.
// Non-IPv4 packets transition directly to accept; all subsequent Ingress
// Pipeline logic is skipped because hdr.ipv4.isValid() will be false.
// No metadata fields are initialised here; all writes are performed in
// the Ingress Pipeline.
parser MyParser(packet_in packet,
                out headers hdr,
                inout metadata meta,
                inout standard_metadata_t standard_metadata) {

    state start {
        transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
        }
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition accept;
    }
}

/*************************************************************************
************   C H E C K S U M    V E R I F I C A T I O N   *************
*************************************************************************/

control MyVerifyChecksum(inout headers hdr, inout metadata meta) {
    apply { }
}

/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

// The Ingress Pipeline implements the Color Marker Bank defined in Section 3.3.1.
// It performs two sequential steps for every arriving IPv4 packet:
//
//  1. VN Classification (ipv4_lpm + vn_classifier):
//     ipv4_lpm must execute first because the leaky-bucket index
//     (bucket_index = vn_id × 8 + egress_spec) depends on egress_spec,
//     which is written by ipv4_lpm.  Reversing the order would cause
//     bucket_index to be computed from an uninitialised value.
//     vn_classifier then performs an exact match on (srcAddr, dstAddr) to
//     assign vn_id and the guaranteed-bandwidth parameter (leak_rate) for
//     the leaky bucket.  A two-field key is used so that traffic from the
//     same host to different destinations can belong to different VNs.
//
//  2. Per-(VN, egress-port) Leaky Bucket (conformance marking):
//     Each (vn_id, egress_spec) pair maintains an independent leaky bucket
//     stored in LCT_reg and X_reg.  The bucket drains at leak_rate Mbps.
//     A packet is green if the virtual water level X' does not exceed τ
//     after adding the packet's bits; otherwise it is yellow.
//     Yellow packets do not update the registers: the water level already
//     exceeds τ, so the arrival is implicitly captured by that excess, and
//     the next conforming packet will still compute the correct decay from
//     the stored LCT.
//
// Results (vn_id and color) are written into user-defined metadata and
// carried transparently to the Traffic Manager, which reads them via
// SSWITCH_VN_QUEUEING_SRC and SSWITCH_COLOR_SRC respectively.
control MyIngress(inout headers hdr,
                  inout metadata meta,
                  inout standard_metadata_t standard_metadata) {

    // Local variables for VN classification
    bit<8>  vn_id     = 0;  // VN identifier resolved by vn_classifier
    int<48> leak_rate = 0;  // Guaranteed bandwidth g_i (Mbps) used as the bucket drain rate

    // Leaky-bucket index: encodes (vn_id, egress_spec) as vn_id × 8 + egress_spec.
    // This supports up to 8 VNs × 8 ports = 64 independent buckets.
    // Forward and reverse flows of the same VN (e.g. H1→H4 and H4→H1) land on
    // different egress ports and therefore use completely independent buckets.
    bit<8>  bucket_index;

    // Leaky-bucket state variables (all quantities in bits unless noted)
    int<48> t_a;      // Arrival timestamp of the current packet (microseconds)
    int<48> LCT;      // Last Conformance Time: timestamp of the last green packet
    int<48> X;        // Virtual water level recorded at the last green packet
    int<48> X_prime;  // Decayed water level at the current packet's arrival
    int<48> T;        // Bit length of the current packet

    // Per-(VN, egress-port) leaky-bucket state registers, indexed by bucket_index.
    // LCT_reg stores the Last Conformance Time (microseconds).
    // X_reg stores the virtual water level after the last green packet (bits).
    // Both registers must always be read and written together to keep their
    // states consistent.
    register<int<48>, bit<8>>(64) LCT_reg;
    register<int<48>, bit<8>>(64) X_reg;

    action drop() {
        mark_to_drop(standard_metadata);
    }

    // Standard IPv4 forwarding: set the output port, rewrite MAC addresses,
    // and decrement TTL.  Must execute before vn_classifier so that
    // egress_spec is available when computing bucket_index.
    action ipv4_forward(macAddr_t dstAddr, egressSpec_t port) {
        standard_metadata.egress_spec = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }

    // Assign the VN identifier and the corresponding guaranteed-bandwidth
    // parameter (leak_rate) to local variables for use in the leaky bucket.
    action set_vn_params(bit<8> vn_id_param, int<48> guaranteed_bandwidth_param) {
        vn_id     = vn_id_param;
        leak_rate = guaranteed_bandwidth_param;
    }

    // Longest-prefix match on destination IP to determine the output port.
    // This table must be applied before vn_classifier.
    table ipv4_lpm {
        key = {
            hdr.ipv4.dstAddr: lpm;
        }
        actions = {
            ipv4_forward;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = drop();
    }

    // Exact match on (srcAddr, dstAddr) to classify a packet into a VN and
    // retrieve its guaranteed-bandwidth parameter.  A two-field key allows
    // traffic from the same source host to different destinations to be
    // assigned to different VNs.  Bidirectional flows (e.g. H1→H4 and
    // H4→H1) must each have their own table entry with the same vn_id and
    // leak_rate; they remain independent because bucket_index encodes
    // egress_spec, so forward and reverse traffic use separate buckets.
    table vn_classifier {
        key = {
            hdr.ipv4.srcAddr: exact;
            hdr.ipv4.dstAddr: exact;
        }
        actions = {
            set_vn_params;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    apply {
        if (hdr.ipv4.isValid()) {
            // Step 1: Determine the output port.  egress_spec must be set
            // before computing bucket_index below.
            ipv4_lpm.apply();

            if (vn_classifier.apply().hit) {
                // Packet belongs to a known VN; proceed with leaky-bucket marking.
                log_msg("########## vn_classifier table hit: The packet's VN ID is {}, and the VN's guaranteed bandwidth is {} Mbps.", {vn_id, leak_rate});

                // Write vn_id into metadata so that the Traffic Manager can
                // route this packet to the correct per-VN FIFO queue.
                meta.vn_id = vn_id;
                log_msg("########## Write the VN ID {} into the user-defined metadata.", {meta.vn_id});

                // Step 2: Leaky-bucket conformance check.

                // Compute the leaky-bucket index for this (VN, egress port) pair.
                bucket_index = vn_id * 8 + (bit<8>)standard_metadata.egress_spec;

                // Read the current arrival timestamp (microseconds).
                t_a = (int<48>) standard_metadata.ingress_global_timestamp;
                log_msg("########## Current time is {} us.", {t_a});

                // Read the stored leaky-bucket state for this bucket.
                LCT_reg.read(LCT, bucket_index);

                // On the very first packet for this bucket, initialise LCT
                // to the current timestamp to avoid a spurious large decay.
                if (LCT == 0) {
                    LCT = t_a;
                }

                // Compute the decayed water level: the bucket drains at
                // leak_rate Mbps, so elapsed_us × leak_rate gives bits drained.
                // (elapsed in µs) × (Mbps) = bits, no unit conversion needed.
                X_reg.read(X, bucket_index);
                X_prime = X - (t_a - LCT) * leak_rate;
                log_msg("########## Compute the bucket's X_prime = {}.", {X_prime});

                if (X_prime > tau) {
                    // Water level exceeds the burst limit τ: mark yellow.
                    // The registers are NOT updated for yellow packets.
                    // The excess water level already captures this arrival;
                    // the next green packet will compute the correct decay
                    // from the unchanged LCT.
                    log_msg("########## The bucket marks this packet as yellow.");
                    meta.color = 1;
                    log_msg("########## color = {}", {meta.color});
                } else {
                    // Water level is within τ: mark green and update the bucket.
                    log_msg("########## The bucket marks this packet as green.");
                    meta.color = 0;
                    log_msg("########## color = {}", {meta.color});

                    // Add this packet's bits to the water level.
                    // If X_prime is negative (bucket over-drained), start from
                    // zero to prevent underestimating future water levels.
                    T = (int<48>)(bit<48>)(standard_metadata.packet_length * 8);
                    log_msg("########## packet length = {} bit", {T});

                    X_prime = (X_prime < 0) ? T : X_prime + T;
                    X_reg.write(bucket_index, X_prime);

                    // Update LCT to the current arrival time so that the next
                    // packet's decay is computed from this conforming event.
                    LCT_reg.write(bucket_index, t_a);
                }
            } else {
                // Packet does not match any VN entry; fall back to VN 3 (the
                // default VN) and mark it green so it can be forwarded normally.
                log_msg("########## vn_classifier table miss: assigning packet to default VN 3.");
                meta.vn_id = 3;
                meta.color = 0;
                log_msg("########## vn_id = {}", {meta.vn_id});
                log_msg("########## color = {}", {meta.color});
            }
        }
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

// The Egress Pipeline is intentionally left empty.
// vn_id and color have already been consumed by the Traffic Manager at
// enqueue time; no further processing is required before deparsing.
control MyEgress(inout headers hdr,
                 inout metadata meta,
                 inout standard_metadata_t standard_metadata) {
    apply { }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   **************
*************************************************************************/

control MyComputeChecksum(inout headers hdr, inout metadata meta) {
    apply {
        update_checksum(
            hdr.ipv4.isValid(),
            { hdr.ipv4.version,
              hdr.ipv4.ihl,
              hdr.ipv4.diffserv,
              hdr.ipv4.totalLen,
              hdr.ipv4.identification,
              hdr.ipv4.flags,
              hdr.ipv4.fragOffset,
              hdr.ipv4.ttl,
              hdr.ipv4.protocol,
              hdr.ipv4.srcAddr,
              hdr.ipv4.dstAddr },
            hdr.ipv4.hdrChecksum,
            HashAlgorithm.csum16);
    }
}

/*************************************************************************
***********************  D E P A R S E R  ********************************
*************************************************************************/

// Emit Ethernet and IPv4 headers in order; the payload follows implicitly.
control MyDeparser(packet_out packet, in headers hdr) {
    apply {
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
    }
}

/*************************************************************************
***********************  S W I T C H  ************************************
*************************************************************************/

V1Switch(
    MyParser(),
    MyVerifyChecksum(),
    MyIngress(),
    MyEgress(),
    MyComputeChecksum(),
    MyDeparser()
) main;