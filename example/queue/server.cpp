// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>              // DEFINE_*
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "queue.pb.h"                 // QueueService
#include <map>

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "Queue", "Id of the replication group");

namespace example {
class Queue;

// Implements Closure which encloses RPC stuff
class PushClosure : public braft::Closure {
public:
    PushClosure(Queue* queue, 
                const PushRequest* request,
                PushResponse* response,
                google::protobuf::Closure* done)
        : _queue(queue)
        , _request(request)
        , _response(response)
        , _done(done) {}
    ~PushClosure() {}

    const PushRequest* request() const { return _request; }
    PushResponse* response() const { return _response; }
    void Run();

private:
    Queue* _queue;
    const PushRequest* _request;
    PushResponse* _response;
    google::protobuf::Closure* _done;
};

// Implementation of example::Queue as a braft::StateMachine.
class Queue : public braft::StateMachine {
public:
    Queue()
        : _seq(0)
        , _current_index(0)
        , _node(NULL)
        , _leader_term(-1)
    {}
    ~Queue() {
        delete _node;
    }

    // Starts this node
    int start() {
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    // Impelements Service methods
    void push(const PushRequest* request,
                   PushResponse* response,
                   google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Serialize request to the replicated write-ahead-log so that all the
        // peers in the group receive this request as well.
        // Notice that _value can't be modified in this routine otherwise it
        // will be inconsistent with others in this group.
        
        // Serialize request to IOBuf
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            LOG(WARNING) << "Not the leader";
            return redirect(response);
        }
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new PushClosure(this, request, response,
                                        done_guard.release());
        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        LOG(INFO) << "Start apply request:" << request->cid();
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    // void get(QueueResponse* response) {
    //     // In consideration of consistency. GetRequest to follower should be 
    //     // rejected.
    //     if (!is_leader()) {
    //         // This node is a follower or it's not up-to-date. Redirect to
    //         // the leader if possible.
    //         return redirect(response);
    //     }

    //     // This is the leader and is up-to-date. It's safe to respond client
    //     response->set_success(true);
    //     response->set_value(_value.load(butil::memory_order_relaxed));
    // }

    bool is_leader() const 
    { return _leader_term.load(butil::memory_order_acquire) > 0; }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
friend class PushClosure;

    void redirect(PushResponse* response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
        //     if (!leader.is_empty()) {
        //         response->set_redirect(leader.to_string());
        //     }
        }
    }

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through 
        // |iter|
        for (; iter.valid(); iter.next()) {
            LOG(INFO) << "on_apply index:" << iter.index();
            bool result = false;
            int64_t cid = 0;
            PushResponse* response = NULL;
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            braft::AsyncClosureGuard closure_guard(iter.done());
            if (iter.done()) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                PushClosure* c = dynamic_cast<PushClosure*>(iter.done());
                LOG(INFO) << "leader on_apply index:" << iter.index() << " cid:" << c->request()->cid();
                result = insert(c->request()->cid(), c->request()->value());
                cid = c->request()->cid();
                response = c->response();
            } else {
                // Have to parse PushRequest from this log.
                butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
                PushRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                LOG(INFO) << "follower on_apply index:" << iter.index() << " cid:" << request.cid();
                result = insert(request.cid(), request.value());
                cid = request.cid();
            }

            // Now the log has been parsed. Update this state machine by this
            // operation.
            
            if (response) {
                response->set_success(result);
                response->set_cid(cid);
                response->set_did(_seq);
            }
            _current_index = iter.index();
            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "push cid=" << cid << " by seq=" << _seq
                    << " at log_index=" << iter.index();
        }
    }

    struct SnapshotArg {
        Snapshot snap;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg) {
        SnapshotArg* sa = (SnapshotArg*) arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        // Serialize StateMachine to the snapshot
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/data";
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        // Use protobuf to store the snapshot for backward compatibility.
        braft::ProtoBufFile pb_file(snapshot_path);
        if (pb_file.save(&sa->snap, true) != 0)  {
            sa->done->status().set_error(EIO, "Fail to save pb_file");
            return NULL;
        }
        // Snapshot is a set of files in raft. Add the only file into the
        // writer here.
        if (sa->writer->add_file("data") != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer");
            return NULL;
        }
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotArg* arg = new SnapshotArg;
        arg->snap.set_seq(_seq);
        arg->snap.set_index(_current_index);
        // arg->snap.set_timestamp();
        for (auto& item : _data) {
            QData* qdata = arg->snap.add_datas();
            qdata->set_did(item.second->did());
            qdata->set_cid(item.second->cid());
            qdata->set_get_count(item.second->get_count());
            qdata->set_raw_data(item.second->raw_data());
        }
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        // Load snasphot from reader, replacing the running StateMachine
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        if (reader->get_file_meta("data", NULL) != 0) {
            LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
            return -1;
        }
        std::string snapshot_path = reader->get_path() + "/data";
        braft::ProtoBufFile pb_file(snapshot_path);
        Snapshot snap;
        if (pb_file.load(&snap) != 0) {
            LOG(ERROR) << "Fail to load snapshot from " << snapshot_path;
            return -1;
        }

        _seq = snap.seq();
        _current_index = snap.index();
        auto it = _data.begin();
        while (it != _data.end()) {
            delete it->second;
            it = _data.erase(it);
        }        
        
        for (int i = 0; i < snap.datas_size(); i++) {
            const QData& qdata = snap.datas(i);
            QData* data = new QData;
            data->set_did(qdata.did());
            data->set_cid(qdata.cid());
            data->set_push_time(qdata.push_time());
            data->set_get_count(qdata.get_count());
            data->set_raw_data(qdata.raw_data());
            _data.insert(std::make_pair(data->cid(), data));
        }

        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine
private:
    bool insert(int64_t cid, const std::string& data) {
        auto it = _data.find(cid);
        if (it != _data.end()) {
            return false;
        }

        _seq++;
        QData* qdata = new QData;
        qdata->set_did(_seq);
        qdata->set_cid(cid);
        // qdata->push_time = ;  时间策略需要想办法
        qdata->set_get_count(0);
        qdata->set_raw_data(data);

        _data.insert(std::make_pair(cid, qdata));
        // TODO FOT TEST
        if (_data.size() > 10000) {
            auto rit = _data.erase(_data.begin());
            delete rit->second;
        }
        return true;
    }

private:
    int64_t _seq;
    int64_t _current_index;
    std::map<int64_t, QData*> _data;
    braft::Node* volatile _node;
    // butil::atomic<int64_t> _value;
    butil::atomic<int64_t> _leader_term;
};

void PushClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<PushClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    _queue->redirect(_response);
}

// Implements example::QueueService if you are using brpc.
class QueueServiceImpl : public QueueService {
public:
    explicit QueueServiceImpl(Queue* queue) : _queue(queue) {}
    void create(::google::protobuf::RpcController* controller,
             const ::example::CreateRequest* request,
             ::example::CreateResponse* response,
             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // return _queue->get(response);
    }
    void del(::google::protobuf::RpcController* controller,
             const ::example::DeleteRequest* request,
             ::example::DeleteResponse* response,
             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // return _queue->get(response);
    }
    void push(::google::protobuf::RpcController* controller,
             const ::example::PushRequest* request,
             ::example::PushResponse* response,
             ::google::protobuf::Closure* done) {
        LOG(INFO) << "Recv push request:" << request->cid();
        return _queue->push(request, response, done);
    }
    void get(::google::protobuf::RpcController* controller,
             const ::example::GetRequest* request,
             ::example::GetResponse* response,
             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // return _queue->get(response);
    }
    void pop(::google::protobuf::RpcController* controller,
                   const ::example::PopRequest* request,
                   ::example::PopResponse* response,
                   ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // return _queue->pop(request, response, done);
    }
private:
    Queue* _queue;
};

};  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    example::Queue queue;
    example::QueueServiceImpl service(&queue);

    // Add your service into RPC server
    if (server.AddService(&service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Queue is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start Queue;
    if (queue.start() != 0) {
        LOG(ERROR) << "Fail to start Queue";
        return -1;
    }

    LOG(INFO) << "Queue service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "Queue service is going to quit";

    // Stop counter before server
    queue.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    queue.join();
    server.Join();
    return 0;
}
