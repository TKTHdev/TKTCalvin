// Author: Kun Ren <renkun.nwpu@gmail.com>
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>

#include "machine/cluster_manager.h"

DEFINE_string(command, "status", "cluster command");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_string(calvin_path, "/home/ubuntu/CalvinDB", "path to the main calvin directory");
DEFINE_string(binary, "calvindb_server", "Calvin binary executable program");
DEFINE_string(lowlatency_binary, "lowlatency_calvindb_server", "Lowlatency Calvin binary executable program");
DEFINE_string(ssh_key, "-i ~/.ssh/id_rsa", "SSH key for all servers");
DEFINE_int32(lowlatency, 0, "0: Original CalvinDB ; 1: low latency version of CalvinDB; 2: low latency with access pattern remasters");
DEFINE_int32(type, 0, "Reserved for low latency mode: 0: normal; 1: normal; 2: strong availability");
DEFINE_int32(experiment, 0, "the experiment that you want to run, default is microbenchmark");
DEFINE_int32(percent_mp, 0, "percent of distributed txns");
DEFINE_int32(percent_mr, 0, "percent of multi-replica txns");
DEFINE_int32(hot_records, 10000, "number of hot records--to control contention");
DEFINE_int32(max_batch_size, 100, "max batch size of txns per epoch");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  ClusterManager* cm;
  if (FLAGS_lowlatency == 0) {
    cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key);
  } else {
    cm = new ClusterManager(FLAGS_config, FLAGS_calvin_path, FLAGS_lowlatency_binary, FLAGS_lowlatency, FLAGS_type, FLAGS_ssh_key);
  }

  if (FLAGS_command == "update") {
    cm->Update();

  } else if (FLAGS_command == "put-config") {
    cm->PutConfig();

  } else if (FLAGS_command == "get-data") {
    cm->GetTempFiles("report.");

  } else if (FLAGS_command == "start") {
    cm->DeployCluster(FLAGS_experiment, FLAGS_percent_mp, FLAGS_percent_mr, FLAGS_hot_records, FLAGS_max_batch_size);

  } else if (FLAGS_command == "kill") {
    cm->KillCluster();

  } else if (FLAGS_command == "status") {
    cm->ClusterStatus();

  } else {
    LOG(FATAL) << "unknown command: " << FLAGS_command;
  }
  return 0;
}

