#ifndef UTILS_H
#define UTILS_H

#include <arpa/inet.h>
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace std;

struct QPInfo {
    uint16_t lid;
    uint32_t qp_num;
    char gid_raw[16]; // 存储GID的原始字节
} __attribute__((packed));

vector<string> split(const string &s, const string &delim);

int post_send(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data, struct ibv_qp *qp, char *buf,
              const string thread_id = "", const string line = "");
int post_srq_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_srq *srq, char *buf,
                  const string thread_id = "", const string line = "");
int post_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf,
              const string thread_id = "", const string line = "");
int poll_completion(int thread_index, struct ibv_cq *cq, enum ibv_wc_opcode target_opcode, int line);
void wait_completion(ibv_comp_channel *comp_channel, ibv_cq *cq, enum ibv_wc_opcode target_opcode, int target, int line);
int post_read(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey,
              const string thread_id = "", const string line = "");
int post_write_with_imm(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data,
                        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, const string thread_id = "", const string line = "");

int sock_read(int sock_fd, char *buf, size_t len);
int sock_write(int sock_fd, char *buf, size_t len);

// 辅助函数
void print_device_info(struct ibv_context *ctx, int port_num);
void print_gid(const char *prefix, const char *gid_raw);

#endif