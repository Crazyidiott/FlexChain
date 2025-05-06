#include "utils.h"

#include "log.h"

vector<string> split(const string &s, const string &delim) {
    vector<string> results;
    if (s == "") {
        return results;
    }
    size_t prev = 0, cur = 0;
    do {
        cur = s.find(delim, prev);
        if (cur == string::npos) {
            cur = s.length();
        }
        string part = s.substr(prev, cur - prev);
        auto no_space_end = remove(part.begin(), part.end(), ' ');
        part.erase(no_space_end, part.end());
        if (!part.empty()) {
            results.emplace_back(part);
        }
        prev = cur + delim.length();
    } while (cur < s.length() && prev < s.length());
    return results;
}

int sock_read(int sock_fd, char *buf, size_t len) {
    int rcvd = 0;
    while (rcvd < len) {
        int n = read(sock_fd, &buf[rcvd], len - rcvd);
        if (n < 0) {
            return -1;
        }
        rcvd += n;
    }
    return rcvd;
}

int sock_write(int sock_fd, char *buf, size_t len) {
    int sent = 0;
    while (sent < len) {
        int n = write(sock_fd, &buf[sent], len - sent);
        if (n < 0) {
            return -1;
        }
        sent += n;
    }
    return sent;
}

int post_srq_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
                  struct ibv_srq *srq, char *buf, const string thread_id, const string line) {
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_recv_wr recv_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1};

    ret = ibv_post_srq_recv(srq, &recv_wr, &bad_recv_wr);
    if (ret) {
        log_err("thread[%s]:%s: ibv_post_srq_recv failed with status '%s'.", thread_id.c_str(), line.c_str(), strerror(errno));
    }
    return ret;
}

int post_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              struct ibv_qp *qp, char *buf, const string thread_id, const string line) {
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_recv_wr recv_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1};

    ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
    if (ret) {
        log_err("thread[%s]:%s: ibv_post_recv failed with status '%s'.", thread_id.c_str(), line.c_str(), strerror(errno));
    }
    return ret;
}

int post_send(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              uint32_t imm_data, struct ibv_qp *qp, char *buf, const string thread_id, const string line) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_SEND_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(imm_data)};

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret) {
        log_err("thread[%s]:%s: ibv_post_send failed with status '%s'.", thread_id.c_str(), line.c_str(), strerror(errno));
    }
    return ret;
}

int post_write_with_imm(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data,
                        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, const string thread_id, const string line) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(imm_data),
        .wr = {
            .rdma = {
                .remote_addr = raddr,
                .rkey = rkey,
            }}};

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret) {
        log_err("thread[%s]:%s: ibv_post_send (RDMA_WRITE_WITH_IMM) failed with status '%s'.", thread_id.c_str(), line.c_str(), strerror(errno));
    }
    return ret;
}

int post_read(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, const string thread_id, const string line) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = raddr,
                .rkey = rkey,
            }}};

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret) {
        log_err("thread[%s]:%s: ibv_post_send (RDMA_READ) failed with status '%s'.", thread_id.c_str(), line.c_str(), strerror(errno));
    }
    return ret;
}

int poll_completion(int thread_index, struct ibv_cq *cq, enum ibv_wc_opcode target_opcode, int line) {
    int num_wc = 20;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *)calloc(num_wc, sizeof(struct ibv_wc));
    int target_count = 0;
    while (true) {
        int n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0) {
            log_err("thread[%d]:%d: failed to poll cq.", thread_index, line);
            free(wc);
            return -1;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    log_err("thread[%d]:%d: send failed with status '%s'.", thread_index, line, ibv_wc_status_str(wc[i].status));
                } else {
                    log_err("thread[%d]:%d: recv failed with status '%s'.", thread_index, line, ibv_wc_status_str(wc[i].status));
                }
                free(wc);
                return -1;
            }

            if (wc[i].opcode == target_opcode) {
                target_count++;
            }
        }
        if (target_count) {
            free(wc);
            return target_count;
        }
    }
}

/* blocks the calling thread until work completion */
void wait_completion(ibv_comp_channel *comp_channel, ibv_cq *cq, enum ibv_wc_opcode target_opcode, int target, int line) {
    int target_count = 0;
    while (target_count < target) {
        struct ibv_cq *ev_cq;
        void *ev_ctx;

        if (ibv_get_cq_event(comp_channel, &ev_cq, &ev_ctx)) {
            log_err("line %d: failed to wait for a completion event.", line);
        }

        ibv_ack_cq_events(cq, 1);

        if (ibv_req_notify_cq(cq, 0)) {
            log_err("line %d: failed to request for a notification.", line);
        }

        int ne;
        struct ibv_wc wc;
        do {
            ne = ibv_poll_cq(cq, 1, &wc);
            if (ne == 0) {
                continue;
            }
            if (ne < 0) {
                log_err("line %d: failed to poll bg_cq.", line);
                break;
            }
            if (wc.status != IBV_WC_SUCCESS) {
                log_err("line %d: WC failed with status %s in bg_cq.", line, ibv_wc_status_str(wc.status));
                break;
            } else {
                if (wc.opcode == target_opcode) {
                    target_count++;
                }
            }
        } while (ne);
    }
}

void print_device_info(struct ibv_context *ctx, int port_num) {
    struct ibv_device_attr device_attr;
    struct ibv_port_attr port_attr;
    
    if (ibv_query_device(ctx, &device_attr)) {
        log_err("Failed to query device attributes");
        return;
    }
    
    if (ibv_query_port(ctx, port_num, &port_attr)) {
        log_err("Failed to query port attributes");
        return;
    }
    
    log_info(stderr, "Device: %s", ibv_get_device_name(ctx->device));
    log_info(stderr, "  fw_ver: %s", device_attr.fw_ver);
    log_info(stderr, "  node_guid: %016" PRIx64, be64toh(device_attr.node_guid));
    log_info(stderr, "  max_qp: %d", device_attr.max_qp);
    log_info(stderr, "  max_cq: %d", device_attr.max_cq);
    log_info(stderr, "  max_mr: %d", device_attr.max_mr);
    log_info(stderr, "  max_pd: %d", device_attr.max_pd);
    log_info(stderr, "  max_qp_wr: %d", device_attr.max_qp_wr);
    
    log_info(stderr, "Port %d:", port_num);
    log_info(stderr, "  state: %d", port_attr.state);
    log_info(stderr, "  max_mtu: %d", port_attr.max_mtu);
    log_info(stderr, "  active_mtu: %d", port_attr.active_mtu);
    log_info(stderr, "  lid: %d", port_attr.lid);
    log_info(stderr, "  sm_lid: %d", port_attr.sm_lid);
    
    // 打印端口上的所有GID
    for (int i = 0; i < 1; i++) { // 通常只使用第一个GID
        union ibv_gid gid;
        if (!ibv_query_gid(ctx, port_num, i, &gid)) {
            log_info(stderr, "  gid[%d]: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
                    i,
                    gid.raw[0], gid.raw[1], gid.raw[2], gid.raw[3], 
                    gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7],
                    gid.raw[8], gid.raw[9], gid.raw[10], gid.raw[11],
                    gid.raw[12], gid.raw[13], gid.raw[14], gid.raw[15]);
        }
    }
}

void print_gid(const char *prefix, const char *gid_raw) {
    log_info(stderr, "%s: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
            prefix,
            (unsigned char)gid_raw[0], (unsigned char)gid_raw[1], 
            (unsigned char)gid_raw[2], (unsigned char)gid_raw[3],
            (unsigned char)gid_raw[4], (unsigned char)gid_raw[5], 
            (unsigned char)gid_raw[6], (unsigned char)gid_raw[7],
            (unsigned char)gid_raw[8], (unsigned char)gid_raw[9], 
            (unsigned char)gid_raw[10], (unsigned char)gid_raw[11],
            (unsigned char)gid_raw[12], (unsigned char)gid_raw[13], 
            (unsigned char)gid_raw[14], (unsigned char)gid_raw[15]);
}