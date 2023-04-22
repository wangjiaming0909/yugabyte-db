// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "yb/common/schema.h"
#include "yb/common/wire_protocol-test-util.h"

#include "yb/consensus/consensus-test-util.h"
#include "yb/consensus/consensus_types.h"
#include "yb/consensus/log.h"
#include "yb/consensus/peer_manager.h"

#include "yb/consensus/state_change_context.h"
#include "yb/consensus/consensus.service.h"

#include "yb/fs/fs_manager.h"

#include "yb/gutil/bind.h"
#include "yb/gutil/stl_util.h"

#include "yb/rpc/connection_context.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/rpc_context.h"
#include "yb/rpc/yb_rpc.h"
#include "yb/server/logical_clock.h"

#include "yb/server/rpc_server.h"
#include "yb/tserver/service_util.h"
#include "yb/util/async_util.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/metrics.h"
#include "yb/util/status_log.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(never_fsync);

METRIC_DECLARE_entity(table);
METRIC_DECLARE_entity(tablet);

using std::shared_ptr;
using std::string;
using std::vector;

namespace yb {
namespace consensus {

using log::Log;
using log::LogOptions;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::AtLeast;
using ::testing::Eq;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::Property;
using ::testing::Return;

const char* kTestTable = "TestTable";
const char* kTestTablet = "TestTablet";
const char* kLocalPeerUuid = "peer-0";

// A simple map to collect the results of a sequence of transactions.
typedef std::map<OpIdPB, Status, OpIdCompareFunctor> StatusesMap;

class MockQueue : public PeerMessageQueue {
 public:
  explicit MockQueue(const scoped_refptr<MetricEntity>& tablet_metric_entity, log::Log* log,
                     const server::ClockPtr& clock,
                     std::unique_ptr<ThreadPoolToken> raft_pool_observers_token)
      : PeerMessageQueue(
          tablet_metric_entity, log, nullptr /* server_tracker */, nullptr /* parent_tracker */,
          FakeRaftPeerPB(kLocalPeerUuid), kTestTablet, clock, nullptr /* consensus_queue */,
          std::move(raft_pool_observers_token)) {}

  MOCK_METHOD1(Init, void(const OpId& locally_replicated_index));
  MOCK_METHOD4(SetLeaderMode, void(const OpId& committed_opid,
                                   int64_t current_term,
                                   const OpId& last_applied_op_id,
                                   const RaftConfigPB& active_config));
  MOCK_METHOD0(SetNonLeaderMode, void());
  Status AppendOperations(const ReplicateMsgs& msgs,
                          const yb::OpId& committed_op_id,
                          RestartSafeCoarseTimePoint time) override {
    return AppendOperationsMock(msgs, committed_op_id, time);
  }
  MOCK_METHOD3(AppendOperationsMock, Status(const ReplicateMsgs& msgs,
                                            const yb::OpId& committed_op_id,
                                            RestartSafeCoarseTimePoint time));
  MOCK_METHOD1(TrackPeer, void(const string&));
  MOCK_METHOD1(UntrackPeer, void(const string&));
  MOCK_METHOD6(RequestForPeer, Status(const std::string& uuid,
                                      LWConsensusRequestPB* request,
                                      LWReplicateMsgsHolder* msgs_holder,
                                      bool* needs_remote_bootstrap,
                                      PeerMemberType* member_type,
                                      bool* last_exchange_successful));
  MOCK_METHOD2(ResponseFromPeer, bool(const std::string& peer_uuid,
                                      const LWConsensusResponsePB& response));
  MOCK_METHOD0(Close, void());
};

class MockPeerManager : public PeerManager {
 public:
  MockPeerManager() : PeerManager("", "", nullptr, nullptr, nullptr, nullptr) {}
  MOCK_METHOD1(UpdateRaftConfig, void(const consensus::RaftConfigPB& config));
  MOCK_METHOD1(SignalRequest, void(RequestTriggerMode trigger_mode));
  MOCK_METHOD0(Close, void());
};

class RaftConsensusSpy : public RaftConsensus {
 public:
  typedef Callback<Status(const scoped_refptr<ConsensusRound>& round)> AppendCallback;

  RaftConsensusSpy(const ConsensusOptions& options,
                   std::unique_ptr<ConsensusMetadata> cmeta,
                   std::unique_ptr<PeerProxyFactory> proxy_factory,
                   std::unique_ptr<PeerMessageQueue> queue,
                   std::unique_ptr<PeerManager> peer_manager,
                   std::unique_ptr<ThreadPoolToken> raft_pool_token,
                   const scoped_refptr<MetricEntity>& table_metric_entity,
                   const scoped_refptr<MetricEntity>& tablet_metric_entity,
                   const std::string& peer_uuid,
                   const scoped_refptr<server::Clock>& clock,
                   ConsensusContext* consensus_context,
                   const scoped_refptr<log::Log>& log,
                   const shared_ptr<MemTracker>& parent_mem_tracker,
                   const Callback<void(std::shared_ptr<consensus::StateChangeContext> context)>&
                     mark_dirty_clbk)
    : RaftConsensus(options,
                    std::move(cmeta),
                    std::move(proxy_factory),
                    std::move(queue),
                    std::move(peer_manager),
                    std::move(raft_pool_token),
                    table_metric_entity,
                    tablet_metric_entity,
                    peer_uuid,
                    clock,
                    consensus_context,
                    log,
                    parent_mem_tracker,
                    mark_dirty_clbk,
                    YQL_TABLE_TYPE,
                    nullptr /* retryable_requests */) {
    // These "aliases" allow us to count invocations and assert on them.
    ON_CALL(*this, StartConsensusOnlyRoundUnlocked(_))
        .WillByDefault(Invoke(this,
              &RaftConsensusSpy::StartNonLeaderConsensusRoundUnlockedConcrete));
    ON_CALL(*this, NonTrackedRoundReplicationFinished(_, _, _))
        .WillByDefault(Invoke(this, &RaftConsensusSpy::NonTrackedRoundReplicationFinishedConcrete));
  }

  MOCK_METHOD1(AppendNewRoundToQueueUnlocked, Status(const scoped_refptr<ConsensusRound>& round));
  Status AppendNewRoundToQueueUnlockedConcrete(const scoped_refptr<ConsensusRound>& round) {
    return RaftConsensus::AppendNewRoundToQueueUnlocked(round);
  }

  MOCK_METHOD2(AppendNewRoundsToQueueUnlocked, Status(
      const ConsensusRounds& rounds, size_t* processed_rounds));
  Status AppendNewRoundsToQueueUnlockedConcrete(
      const ConsensusRounds& rounds, size_t* processed_rounds) {
    return RaftConsensus::AppendNewRoundsToQueueUnlocked(rounds, processed_rounds);
  }

  MOCK_METHOD1(StartConsensusOnlyRoundUnlocked, Status(const ReplicateMsgPtr& msg));
  Status StartNonLeaderConsensusRoundUnlockedConcrete(const ReplicateMsgPtr& msg) {
    return RaftConsensus::StartConsensusOnlyRoundUnlocked(msg);
  }

  MOCK_METHOD3(NonTrackedRoundReplicationFinished, void(ConsensusRound* round,
                                                   const StdStatusCallback& client_cb,
                                                   const Status& status));
  void NonTrackedRoundReplicationFinishedConcrete(ConsensusRound* round,
                                             const StdStatusCallback& client_cb,
                                             const Status& status) {
    LOG(INFO) << "Round " << round->id() << " finished with status: " << status;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(RaftConsensusSpy);
};

void DoNothing(std::shared_ptr<consensus::StateChangeContext> context) {
}

class RaftConsensusTest : public YBTest {
 public:
  RaftConsensusTest()
      : clock_(server::LogicalClock::CreateStartingAt(HybridTime(0))),
        table_metric_entity_(
          METRIC_ENTITY_table.Instantiate(&metric_registry_, "raft-consensus-test-table")),
        tablet_metric_entity_(
          METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "raft-consensus-test-tablet")),
        schema_(GetSimpleTestSchema()) {
    FLAGS_enable_leader_failure_detection = false;
    options_.tablet_id = kTestTablet;
  }

  void SetUp() override {
    YBTest::SetUp();

    LogOptions options;
    string test_path = GetTestPath("test-peer-root");

    // TODO mock the Log too, since we're gonna mock the queue
    // monitors and pretty much everything else.
    fs_manager_.reset(new FsManager(env_.get(), test_path, "tserver_test"));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->CheckAndOpenFileSystemRoots());
    fs_manager_->SetTabletPathByDataPath(kTestTablet, fs_manager_->GetDataRootDirs()[0]);
    ASSERT_OK(ThreadPoolBuilder("log").Build(&log_thread_pool_));
    ASSERT_OK(Log::Open(LogOptions(),
                       kTestTablet,
                       fs_manager_->GetFirstTabletWalDirOrDie(kTestTable, kTestTablet),
                       fs_manager_->uuid(),
                       schema_,
                       0, // schema_version
                       nullptr, // table_metric_entity
                       nullptr, // tablet_metric_entity
                       log_thread_pool_.get(),
                       log_thread_pool_.get(),
                       log_thread_pool_.get(),
                       std::numeric_limits<int64_t>::max(), // cdc_min_replicated_index
                       &log_));

    log_->TEST_SetAllOpIdsSafe(true);

    ASSERT_OK(ThreadPoolBuilder("raft-pool").Build(&raft_pool_));
    std::unique_ptr<ThreadPoolToken> raft_pool_token =
        raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);
    queue_ = new MockQueue(tablet_metric_entity_, log_.get(), clock_, std::move(raft_pool_token));
    peer_manager_ = new MockPeerManager;
    operation_factory_.reset(new MockOperationFactory);

    ON_CALL(*queue_, AppendOperationsMock(_, _, _))
        .WillByDefault(Invoke(this, &RaftConsensusTest::AppendToLog));
  }

  void SetUpConsensus(int64_t initial_term = consensus::kMinimumTerm, int num_peers = 1) {
    config_ = BuildRaftConfigPBForTests(num_peers);
    config_.set_opid_index(kInvalidOpIdIndex);

    auto proxy_factory = std::make_unique<LocalTestPeerProxyFactory>(nullptr);

    string peer_uuid = config_.peers(num_peers - 1).permanent_uuid();

    std::unique_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(fs_manager_.get(), kTestTablet, peer_uuid,
                                       config_, initial_term, &cmeta));

    std::unique_ptr<ThreadPoolToken> raft_pool_token =
        raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);

    consensus_.reset(new RaftConsensusSpy(options_,
                                          std::move(cmeta),
                                          std::move(proxy_factory),
                                          std::unique_ptr<PeerMessageQueue>(queue_),
                                          std::unique_ptr<PeerManager>(peer_manager_),
                                          std::move(raft_pool_token),
                                          table_metric_entity_,
                                          tablet_metric_entity_,
                                          peer_uuid,
                                          clock_,
                                          operation_factory_.get(),
                                          log_.get(),
                                          MemTracker::GetRootTracker(),
                                          Bind(&DoNothing)));

    ON_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
        .WillByDefault(Invoke(this, &RaftConsensusTest::MockAppendNewRound));
    ON_CALL(*consensus_.get(), AppendNewRoundsToQueueUnlocked(_, _))
        .WillByDefault(Invoke(this, &RaftConsensusTest::MockAppendNewRounds));
  }

  Status AppendToLog(const ReplicateMsgs& msgs,
                     const yb::OpId& committed_op_id,
                     RestartSafeCoarseTimePoint time) {
    return log_->AsyncAppendReplicates(msgs, committed_op_id, time,
                                       Bind(LogAppendCallback));
  }

  static void LogAppendCallback(const Status& s) {
    ASSERT_OK(s);
  }

  Status MockAppendNewRound(const scoped_refptr<ConsensusRound>& round) {
    return consensus_->AppendNewRoundToQueueUnlockedConcrete(round);
  }

  Status MockAppendNewRounds(const ConsensusRounds& rounds, size_t* processed_rounds) {
    for (const auto& round : rounds) {
      rounds_.push_back(round);
    }
    RETURN_NOT_OK(consensus_->AppendNewRoundsToQueueUnlockedConcrete(rounds, processed_rounds));
    for (const auto& round : rounds) {
      LOG(INFO) << "Round append: " << round->id() << ", ReplicateMsg: "
                << round->replicate_msg()->ShortDebugString();
    }
    return Status::OK();
  }

  void SetUpGeneralExpectations() {
    EXPECT_CALL(*peer_manager_, SignalRequest(_))
        .Times(AnyNumber());
    EXPECT_CALL(*peer_manager_, Close())
        .Times(AtLeast(1));
    EXPECT_CALL(*queue_, Close())
        .Times(1);
    EXPECT_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
        .Times(AnyNumber());
  }

  // Create a ConsensusRequestPB suitable to send to a peer.
  ConsensusRequestPB MakeConsensusRequest(int64_t caller_term,
                                          const string& caller_uuid,
                                          const OpId& preceding_opid);

  // Add a single no-op with the given OpId to a ConsensusRequestPB.
  void AddNoOpToConsensusRequest(LWConsensusRequestPB* request, const OpId& noop_opid);

  scoped_refptr<ConsensusRound> AppendNoOpRound() {
    auto replicate_ptr = rpc::MakeSharedMessage<LWReplicateMsg>();
    replicate_ptr->set_op_type(NO_OP);
    replicate_ptr->set_hybrid_time(clock_->Now().ToUint64());
    scoped_refptr<ConsensusRound> round(new ConsensusRound(consensus_.get(),
                                                           std::move(replicate_ptr)));
    round->SetCallback(MakeNonTrackedRoundCallback(
        round.get(),
        std::bind(&RaftConsensusSpy::NonTrackedRoundReplicationFinished,
                  consensus_.get(), round.get(), &DoNothingStatusCB, std::placeholders::_1)));
    round->BindToTerm(consensus_->TEST_LeaderTerm());

    CHECK_OK(consensus_->TEST_Replicate(round));
    LOG(INFO) << "Appended NO_OP round with opid " << round->id();
    return round;
  }

  void DumpRounds() {
    LOG(INFO) << "Dumping rounds...";
    for (const scoped_refptr<ConsensusRound>& round : rounds_) {
      LOG(INFO) << "Round: OpId " << round->id() << ", ReplicateMsg: "
                << round->replicate_msg()->ShortDebugString();
    }
  }

 protected:
  std::unique_ptr<ThreadPool> raft_pool_;
  ConsensusOptions options_;
  RaftConfigPB config_;
  OpIdPB initial_id_;
  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<ThreadPool> log_thread_pool_;
  scoped_refptr<Log> log_;
  std::unique_ptr<PeerProxyFactory> proxy_factory_;
  scoped_refptr<server::Clock> clock_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> table_metric_entity_;
  scoped_refptr<MetricEntity> tablet_metric_entity_;
  const Schema schema_;
  shared_ptr<RaftConsensusSpy> consensus_;

  vector<scoped_refptr<ConsensusRound> > rounds_;

  // Mocks.
  // NOTE: both 'queue_' and 'peer_manager_' belong to 'consensus_' and may be deleted before
  // the test is.
  MockQueue* queue_;
  MockPeerManager* peer_manager_;
  std::unique_ptr<MockOperationFactory> operation_factory_;
};

ConsensusRequestPB RaftConsensusTest::MakeConsensusRequest(int64_t caller_term,
                                                           const string& caller_uuid,
                                                           const OpId& preceding_opid) {
  ConsensusRequestPB request;
  request.set_caller_term(caller_term);
  request.set_caller_uuid(caller_uuid);
  request.set_tablet_id(kTestTablet);
  preceding_opid.ToPB(request.mutable_preceding_id());
  return request;
}

void RaftConsensusTest::AddNoOpToConsensusRequest(LWConsensusRequestPB* request,
                                                  const OpId& noop_opid) {
  auto* noop_msg = request->add_ops();
  noop_opid.ToPB(noop_msg->mutable_id());
  noop_msg->set_op_type(NO_OP);
  noop_msg->set_hybrid_time(clock_->Now().ToUint64());
  noop_msg->mutable_noop_request();
}

// Tests that the committed index moves along with the majority replicated
// index when the terms are the same.
TEST_F(RaftConsensusTest, TestCommittedIndexWhenInSameTerm) {
  SetUpConsensus();
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateRaftConfig(_))
      .Times(1);
  EXPECT_CALL(*queue_, Init(_))
      .Times(1);
  EXPECT_CALL(*queue_, SetLeaderMode(_, _, _, _))
      .Times(1);
  EXPECT_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
      .Times(1);
  EXPECT_CALL(*consensus_.get(), AppendNewRoundsToQueueUnlocked(_, _))
      .Times(11);
  EXPECT_CALL(*queue_, AppendOperationsMock(_, _, _))
      .Times(22).WillRepeatedly(Return(Status::OK()));

  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));
  ASSERT_OK(consensus_->EmulateElection());

  // Commit the first noop round, created on EmulateElection();
  OpId committed_index;
  OpId last_applied_op_id;
  consensus_->TEST_UpdateMajorityReplicated(
      rounds_[0]->id(), &committed_index, &last_applied_op_id);
  ASSERT_EQ(rounds_[0]->id(), committed_index);
  ASSERT_EQ(last_applied_op_id, rounds_[0]->id());

  // Append 10 rounds
  for (int i = 0; i < 10; i++) {
    scoped_refptr<ConsensusRound> round = AppendNoOpRound();
    // queue reports majority replicated index in the leader's term
    // committed index should move accordingly.
    consensus_->TEST_UpdateMajorityReplicated(
        round->id(), &committed_index, &last_applied_op_id);
    ASSERT_EQ(last_applied_op_id, round->id());
  }
}

// Tests that, when terms change, the commit index only advances when the majority
// replicated index is in the current term.
TEST_F(RaftConsensusTest, TestCommittedIndexWhenTermsChange) {
  SetUpConsensus();
  SetUpGeneralExpectations();
  EXPECT_CALL(*peer_manager_, UpdateRaftConfig(_))
      .Times(2);
  EXPECT_CALL(*queue_, Init(_))
      .Times(1);
  EXPECT_CALL(*queue_, SetLeaderMode(_, _, _, _))
      .Times(2);
  EXPECT_CALL(*consensus_.get(), AppendNewRoundsToQueueUnlocked(_, _))
      .Times(3);
  EXPECT_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
      .Times(2);
  EXPECT_CALL(*queue_, AppendOperationsMock(_, _, _))
      .Times(5).WillRepeatedly(Return(Status::OK()));;

  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));
  ASSERT_OK(consensus_->EmulateElection());

  OpId committed_index;
  OpId last_applied_op_id;
  consensus_->TEST_UpdateMajorityReplicated(
      rounds_[0]->id(), &committed_index, &last_applied_op_id);
  ASSERT_EQ(rounds_[0]->id(), committed_index);
  ASSERT_EQ(last_applied_op_id, rounds_[0]->id());

  // Append another round in the current term (besides the original config round).
  scoped_refptr<ConsensusRound> round = AppendNoOpRound();

  // Now emulate an election, the same guy will be leader but the term
  // will change.
  ASSERT_OK(consensus_->EmulateElection());

  // Now tell consensus that 'round' has been majority replicated, this _shouldn't_
  // advance the committed index, since that belongs to a previous term.
  OpId new_committed_index;
  OpId new_last_applied_op_id;
  consensus_->TEST_UpdateMajorityReplicated(
      round->id(), &new_committed_index, &new_last_applied_op_id);
  ASSERT_EQ(committed_index, new_committed_index);
  ASSERT_EQ(last_applied_op_id, new_last_applied_op_id);

  const scoped_refptr<ConsensusRound>& last_config_round = rounds_[2];

  // Now notify that the last change config was committed, this should advance the
  // commit index to the id of the last change config.
  consensus_->TEST_UpdateMajorityReplicated(
      last_config_round->id(), &committed_index, &last_applied_op_id);

  DumpRounds();
  ASSERT_EQ(last_config_round->id(), committed_index);
  ASSERT_EQ(last_applied_op_id, last_config_round->id());
}

// Asserts that a ConsensusRound has an OpId set in its ReplicateMsg.
MATCHER(HasOpId, "") { return !arg->id().empty(); }

// These matchers assert that a Status object is of a certain type.
MATCHER(IsOk, "") { return arg.ok(); }
MATCHER(IsAborted, "") { return arg.IsAborted(); }

// Tests that consensus is able to handle pending operations. It tests this in two ways:
// - It tests that consensus does the right thing with pending transactions from the WAL.
// - It tests that when a follower gets promoted to leader it does the right thing
//   with the pending operations.
TEST_F(RaftConsensusTest, TestPendingOperations) {
  SetUpConsensus(10);

  // Emulate a stateful system by having a bunch of operations in flight when consensus starts.
  // Specifically we emulate we're on term 10, with 10 operations that have not been committed yet.
  ConsensusBootstrapInfo info;
  info.last_id.set_term(10);
  for (int i = 0; i < 10; i++) {
    auto replicate = rpc::MakeSharedMessage<LWReplicateMsg>();
    replicate->set_op_type(NO_OP);
    info.last_id.set_index(100 + i);
    replicate->mutable_id()->CopyFrom(info.last_id);
    info.orphaned_replicates.push_back(replicate);
  }

  info.last_committed_id.set_term(10);
  info.last_committed_id.set_index(99);

  {
    InSequence dummy;
    // On start we expect 10 NO_OPs to be enqueued.
    EXPECT_CALL(*consensus_.get(), StartConsensusOnlyRoundUnlocked(_))
        .Times(10);

    // Queue gets initted when the peer starts.
    EXPECT_CALL(*queue_, Init(_))
        .Times(1);
  }
  ASSERT_OK(consensus_->Start(info));

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(queue_));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(operation_factory_.get()));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(peer_manager_));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(consensus_.get()));

  // Now we test what this peer does with the pending operations once it's elected leader.
  {
    InSequence dummy;
    // Peer manager gets updated with the new set of peers to send stuff to.
    EXPECT_CALL(*peer_manager_, UpdateRaftConfig(_))
        .Times(1);
    // The no-op should be appended to the queue.
    EXPECT_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
        .Times(1);
    // One more op will be appended for the election.
    EXPECT_CALL(*queue_, AppendOperationsMock(_, _, _))
        .Times(1).WillRepeatedly(Return(Status::OK()));;
  }

  // Emulate an election, this will make this peer become leader and trigger the
  // above set expectations.
  ASSERT_OK(consensus_->EmulateElection());

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(queue_));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(operation_factory_.get()));
  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(peer_manager_));

  // Commit the 10 no-ops from the previous term, along with the one pushed to
  // assert leadership.
  EXPECT_CALL(*consensus_.get(), NonTrackedRoundReplicationFinished(HasOpId(), _, IsOk()))
      .Times(11);
  EXPECT_CALL(*peer_manager_, SignalRequest(_))
      .Times(AnyNumber());
  // In the end peer manager and the queue get closed.
  EXPECT_CALL(*peer_manager_, Close())
      .Times(AtLeast(1));
  EXPECT_CALL(*queue_, Close())
      .Times(1);

  // Now tell consensus all original orphaned replicates were majority replicated.
  // This should not advance the committed index because we haven't replicated
  // anything in the current term.
  OpId committed_index;
  OpId last_applied_op_id;
  consensus_->TEST_UpdateMajorityReplicated(
      OpId::FromPB(info.orphaned_replicates.back()->id()), &committed_index, &last_applied_op_id);
  // Should still be the last committed in the wal.
  ASSERT_EQ(committed_index, OpId::FromPB(info.last_committed_id));
  ASSERT_EQ(last_applied_op_id, OpId::FromPB(info.last_committed_id));

  // Now mark the last operation (the no-op round) as committed.
  // This should advance the committed index, since that round in on our current term,
  // and we should be able to commit all previous rounds.
  OpId cc_round_id = OpId::FromPB(info.orphaned_replicates.back()->id());
  cc_round_id.term = 11;

  // +1 here because index is incremented during emulated election.
  ++cc_round_id.index;
  consensus_->TEST_UpdateMajorityReplicated(cc_round_id, &committed_index, &last_applied_op_id);
  ASSERT_EQ(committed_index, cc_round_id);
  ASSERT_EQ(last_applied_op_id, cc_round_id);
}

MATCHER_P2(RoundHasOpId, term, index, "") {
  LOG(INFO) << "expected: " << MakeOpId(term, index) << ", actual: " << arg->id();
  return arg->id().term == term && arg->id().index == index;
}

// Tests the case where a leader is elected and pushed a sequence of
// operations of which some never get committed. Eventually a new leader in a higher
// term pushes operations that overwrite some of the original indexes.
TEST_F(RaftConsensusTest, TestAbortOperations) {
  SetUpConsensus(1, 2);

  EXPECT_CALL(*consensus_.get(), AppendNewRoundToQueueUnlocked(_))
      .Times(AnyNumber());

  EXPECT_CALL(*peer_manager_, SignalRequest(_))
      .Times(AnyNumber());
  EXPECT_CALL(*peer_manager_, Close())
      .Times(AtLeast(1));
  EXPECT_CALL(*queue_, Close())
      .Times(1);
  EXPECT_CALL(*queue_, Init(_))
      .Times(1);
  EXPECT_CALL(*peer_manager_, UpdateRaftConfig(_))
      .Times(1);

  // We'll append to the queue 12 times, the initial noop txn + 10 initial ops while leader
  // and the new leader's update, when we're overwriting operations.
  EXPECT_CALL(*queue_, AppendOperationsMock(_, _, _))
      .Times(13);

  // .. but those will be overwritten later by another
  // leader, which will push and commit 5 ops.
  // Only these five should start as replica rounds.
  EXPECT_CALL(*consensus_.get(), StartConsensusOnlyRoundUnlocked(_))
      .Times(4);

  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));
  ASSERT_OK(consensus_->EmulateElection());

  // Append 10 rounds: 2.2 - 2.11
  for (int i = 0; i < 10; i++) {
    AppendNoOpRound();
  }

  // Expectations for what gets committed and what gets aborted:
  // (note: the aborts may be triggered before the commits)
  // 5 OK's for the 2.1-2.5 ops.
  // 6 Aborts for the 2.6-2.11 ops.
  // 1 OK for the 3.6 op.
  for (int index = 1; index < 6; index++) {
    EXPECT_CALL(*consensus_.get(),
                NonTrackedRoundReplicationFinished(RoundHasOpId(2, index), _, IsOk())).Times(1);
  }
  for (int index = 6; index < 12; index++) {
    EXPECT_CALL(*consensus_.get(),
                NonTrackedRoundReplicationFinished(
                    RoundHasOpId(2, index), _, IsAborted())).Times(1);
  }
  EXPECT_CALL(*consensus_.get(),
              NonTrackedRoundReplicationFinished(RoundHasOpId(3, 6), _, IsOk())).Times(1);

  // Nothing's committed so far, so now just send an Update() message
  // emulating another guy got elected leader and is overwriting a suffix
  // of the previous messages.
  // In particular this request has:
  // - Op 2.5 from the previous leader's term
  // - Ops 3.6-3.9 from the new leader's term
  // - A new committed index of 3.6
  auto request_ptr = rpc::MakeSharedMessage<LWConsensusRequestPB>();
  auto& request = *request_ptr;
  request.set_caller_term(3);
  const string PEER_0_UUID = "peer-0";
  request.ref_caller_uuid(PEER_0_UUID);
  request.ref_tablet_id(kTestTablet);
  request.mutable_preceding_id()->CopyFrom(MakeOpId(2, 4));

  auto* replicate = request.add_ops();
  replicate->mutable_id()->CopyFrom(MakeOpId(2, 5));
  replicate->set_op_type(NO_OP);

  auto* noop_msg = request.add_ops();
  noop_msg->mutable_id()->CopyFrom(MakeOpId(3, 6));
  noop_msg->set_op_type(NO_OP);
  noop_msg->set_hybrid_time(clock_->Now().ToUint64());
  noop_msg->mutable_noop_request();

  // Overwrite another 3 of the original rounds for a total of 4 overwrites.
  for (int i = 7; i < 10; i++) {
    auto* replicate = request.add_ops();
    replicate->mutable_id()->CopyFrom(MakeOpId(3, i));
    replicate->set_op_type(NO_OP);
    replicate->set_hybrid_time(clock_->Now().ToUint64());
  }

  request.mutable_committed_op_id()->CopyFrom(MakeOpId(3, 6));

  LWConsensusResponsePB response(&request.arena());
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.has_error());

  ASSERT_TRUE(Mock::VerifyAndClearExpectations(consensus_.get()));

  // Now we expect to commit ops 3.7 - 3.9.
  for (int index = 7; index < 10; index++) {
    EXPECT_CALL(*consensus_.get(),
                NonTrackedRoundReplicationFinished(RoundHasOpId(3, index), _, IsOk())).Times(1);
  }

  request.mutable_ops()->clear();
  request.mutable_preceding_id()->CopyFrom(MakeOpId(3, 9));
  request.mutable_committed_op_id()->CopyFrom(MakeOpId(3, 9));

  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.has_error());
}

TEST_F(RaftConsensusTest, TestReceivedIdIsInittedBeforeStart) {
  SetUpConsensus();
  OpIdPB opid;
  consensus_->GetLastReceivedOpId().ToPB(&opid);
  ASSERT_TRUE(opid.IsInitialized());
  ASSERT_OPID_EQ(opid, MinimumOpId());
}

// Ensure that followers reset their "last_received_current_leader"
// ConsensusStatusPB field when a new term is encountered. This is a
// correctness test for the logic on the follower side that allows the
// leader-side queue to determine which op to send next in various scenarios.
TEST_F(RaftConsensusTest, TestResetRcvdFromCurrentLeaderOnNewTerm) {
  SetUpConsensus(kMinimumTerm, 3);
  SetUpGeneralExpectations();
  ConsensusBootstrapInfo info;
  ASSERT_OK(consensus_->Start(info));

  auto request_ptr = rpc::MakeSharedMessage<LWConsensusRequestPB>();
  auto& request = *request_ptr;
  LWConsensusResponsePB response(&request.arena());
  int64_t caller_term = 0;
  int64_t log_index = 0;

  caller_term = 1;
  string caller_uuid = config_.peers(0).permanent_uuid();
  OpId preceding_opid = OpId::Min();

  // Heartbeat. This will cause the term to increment on the follower.
  request = MakeConsensusRequest(caller_term, caller_uuid, preceding_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(caller_term, response.responder_term());
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), OpId::Min());
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), OpId::Min());

  // Replicate a no-op.
  OpId noop_opid(caller_term, ++log_index);
  AddNoOpToConsensusRequest(&request, noop_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), noop_opid);
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), noop_opid);

  // New leader heartbeat. Term increase to 2.
  // Expect current term replicated to be nothing (MinimumOpId) but log
  // replicated to be everything sent so far.
  caller_term = 2;
  caller_uuid = config_.peers(1).permanent_uuid();
  preceding_opid = noop_opid;
  request = MakeConsensusRequest(caller_term, caller_uuid, preceding_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(caller_term, response.responder_term());
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), preceding_opid);
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), OpId::Min());

  // Append a no-op.
  noop_opid = OpId(caller_term, ++log_index);
  AddNoOpToConsensusRequest(&request, noop_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), noop_opid);
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), noop_opid);

  // New leader heartbeat. The term should rev but we should get an LMP mismatch.
  caller_term = 3;
  caller_uuid = config_.peers(0).permanent_uuid();
  preceding_opid = OpId(caller_term, log_index + 1); // Not replicated yet.
  request = MakeConsensusRequest(caller_term, caller_uuid, preceding_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_EQ(caller_term, response.responder_term());
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), noop_opid); // Not preceding this time.
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), OpId::Min());
  ASSERT_TRUE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH, response.status().error().code());

  // Decrement preceding and append a no-op.
  preceding_opid = OpId(2, log_index);
  noop_opid = OpId(caller_term, ++log_index);
  request = MakeConsensusRequest(caller_term, caller_uuid, preceding_opid);
  AddNoOpToConsensusRequest(&request, noop_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), noop_opid)
      << response.ShortDebugString();
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), noop_opid)
      << response.ShortDebugString();

  // Happy case. New leader with new no-op to append right off the bat.
  // Response should be OK with all last_received* fields equal to the new no-op.
  caller_term = 4;
  caller_uuid = config_.peers(1).permanent_uuid();
  preceding_opid = noop_opid;
  noop_opid = OpId(caller_term, ++log_index);
  request = MakeConsensusRequest(caller_term, caller_uuid, preceding_opid);
  AddNoOpToConsensusRequest(&request, noop_opid);
  response.Clear();
  ASSERT_OK(consensus_->Update(request_ptr, &response, CoarseBigDeadline()));
  ASSERT_FALSE(response.status().has_error()) << response.ShortDebugString();
  ASSERT_EQ(caller_term, response.responder_term());
  ASSERT_EQ(OpId::FromPB(response.status().last_received()), noop_opid);
  ASSERT_EQ(OpId::FromPB(response.status().last_received_current_leader()), noop_opid);
}

class MyConsensusServiceImpl : public  ::yb::consensus::ConsensusServiceIf {
  Consensus* consensus_;

 public:
  MyConsensusServiceImpl(Consensus* consensus, const scoped_refptr<MetricEntity>& metric_entity)
      : ConsensusServiceIf(metric_entity), consensus_(consensus) {}
  void UpdateConsensus(const consensus::LWConsensusRequestPB *req,
                       consensus::LWConsensusResponsePB *resp,
                       rpc::RpcContext context) override {
    auto s = consensus_->Update(
        rpc::SharedField(
            context.shared_params(), const_cast<consensus::LWConsensusRequestPB*>(req)),
        resp, context.GetClientDeadline());
    RETURN_UNKNOWN_ERROR_IF_NOT_OK(s, resp, &context);
    context.RespondSuccess();
  }

  void MultiRaftUpdateConsensus(const consensus::MultiRaftConsensusRequestPB *req,
                                consensus::MultiRaftConsensusResponsePB *resp,
                                rpc::RpcContext context) override {
  }

  void RequestConsensusVote(const consensus::VoteRequestPB* req,
                            consensus::VoteResponsePB* resp,
                            rpc::RpcContext context) override {
    auto s = consensus_->RequestVote(req, resp);
    RETURN_UNKNOWN_ERROR_IF_NOT_OK(s, resp, &context);
    context.RespondSuccess();
  }

  void ChangeConfig(const consensus::ChangeConfigRequestPB* req,
                    consensus::ChangeConfigResponsePB* resp,
                    rpc::RpcContext context) override {}

  void UnsafeChangeConfig(const consensus::UnsafeChangeConfigRequestPB* req,
                          consensus::UnsafeChangeConfigResponsePB* resp,
                          rpc::RpcContext context) override {}

  void GetNodeInstance(const consensus::GetNodeInstanceRequestPB* req,
                       consensus::GetNodeInstanceResponsePB* resp,
                       rpc::RpcContext context) override {}

  void RunLeaderElection(const consensus::RunLeaderElectionRequestPB* req,
                         consensus::RunLeaderElectionResponsePB* resp,
                         rpc::RpcContext context) override {}

  void LeaderElectionLost(const consensus::LeaderElectionLostRequestPB *req,
                          consensus::LeaderElectionLostResponsePB *resp,
                          rpc::RpcContext context) override {}

  void LeaderStepDown(const consensus::LeaderStepDownRequestPB* req,
                      consensus::LeaderStepDownResponsePB* resp,
                      rpc::RpcContext context) override {}

  void GetLastOpId(const consensus::GetLastOpIdRequestPB *req,
                   consensus::GetLastOpIdResponsePB *resp,
                   rpc::RpcContext context) override {}

  void GetConsensusState(const consensus::GetConsensusStateRequestPB *req,
                         consensus::GetConsensusStateResponsePB *resp,
                         rpc::RpcContext context) override {}

  void StartRemoteBootstrap(const consensus::StartRemoteBootstrapRequestPB* req,
                            consensus::StartRemoteBootstrapResponsePB* resp,
                            rpc::RpcContext context) override {}
};

class RaftTest;
struct RaftInstance {
  std::string tablet_uuid_;
  std::string peer_uuid_;
  std::string path_;
  std::string server_type_;
  ConsensusOptions options_;
  RaftConfigPB config_;
  RaftPeerPB local_peer_pb_;
  std::shared_ptr<RaftConsensus> consensus_;
  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<ConsensusMetadata> meta;
  RaftTest* test_;
  scoped_refptr<server::Clock> clock_;
  std::unique_ptr<rpc::Messenger> messenger_;
  std::unique_ptr<rpc::ProxyCache> proxy_cache_;
  std::string tablet_wal_path_;
  std::unique_ptr<Schema> schema_;
  scoped_refptr<Log> log_;

  std::unique_ptr<ThreadPool> thread_pool_;

  std::unique_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> table_metric_entity_;
  scoped_refptr<MetricEntity> tablet_metric_entity_;
  std::unique_ptr<MockOperationFactory> operation_factory_;

  scoped_refptr<MetricEntity> server_metric_entity_;
  std::unique_ptr<server::RpcServer> rpc_server_;
  std::unique_ptr<rpc::Messenger> server_messenger_;
  std::unique_ptr<rpc::ServiceIf> service_impl_;

  RaftInstance(
      const std::string& tablet_uuid, const std::string& uuid, RaftTest* test,
      const ::google::protobuf::RepeatedPtrField<RaftPeerPB>& peers);
  void init();
  void init_log();
  void init_schema();

  void TabletPeerStateChangedCallback(
      const string& tablet_id,
      std::shared_ptr<consensus::StateChangeContext> context) {
    LOG(INFO) << "Tablet peer state changed for tablet " << tablet_id
              << ". Reason: " << context->ToString();
  }
  void start(const ConsensusBootstrapInfo& info) {
    ASSERT_TRUE(consensus_->Start(info).ok());
  }

  void start_rpc_server(const server::RpcServerOptions& opts);
};

class RaftTest : public YBTest {
  std::vector<std::unique_ptr<RaftInstance>> instances_;
  std::unique_ptr<RaftConfigPB> config_;
 public:
  friend struct RaftInstance;
  RaftTest() : instances_(), config_() {
  }
  void set_config(std::unique_ptr<RaftConfigPB> config) {
    config_ = std::move(config);
  }

  void create_instances() {
    auto& peers = config_->peers();
    for (auto it = peers.begin(); it != peers.end(); ++it) {
      std::string tablet_uuid = "tablet-";
      tablet_uuid += it->permanent_uuid();
      instances_.push_back(
          make_unique<RaftInstance>(tablet_uuid, it->permanent_uuid(), this, peers));
    }
  }

  void start_instances() {
    server::RpcServerOptions opt;
    for (auto& instance : instances_) {
      ConsensusBootstrapInfo info;
      instance->start(info);
      opt.default_port = instance->local_peer_pb_.last_known_private_addr(0).port();
      opt.rpc_bind_addresses = instance->local_peer_pb_.last_known_private_addr(0).host();
      instance->start_rpc_server(opt);
    }
  }
};

RaftInstance::RaftInstance(
    const std::string& tablet_uuid, const std::string& uuid, RaftTest* test,
    const ::google::protobuf::RepeatedPtrField<RaftPeerPB>& peers)
    : tablet_uuid_(tablet_uuid), peer_uuid_(uuid), path_("/tmp/"), test_(test) {
  clock_ = server::LogicalClock::CreateStartingAt(HybridTime::kInitial);
  path_ += uuid;
  tablet_wal_path_ = path_ + "/wal";
  options_.tablet_id = tablet_uuid;
  server_type_ = "test-type";
  *config_.mutable_peers() = peers;
  auto self = std::find_if(
      config_.mutable_peers()->begin(), config_.mutable_peers()->end(),
      [&](const RaftPeerPB& peer) { return peer.permanent_uuid() == peer_uuid_; });
  local_peer_pb_ = *self;
  //config.mutable_peers()->erase(self);
  config_.set_opid_index(0);
  operation_factory_.reset(new MockOperationFactory);
  init();
}

void RaftInstance::init() {
  ASSERT_TRUE(ThreadPoolBuilder("log-pool").Build(&thread_pool_).ok());
  metric_registry_.reset(new MetricRegistry());
  fs_manager_.reset(new FsManager(test_->env_.get(), path_, server_type_));
  ASSERT_TRUE(fs_manager_->CreateInitialFileSystemLayout().ok());
  ASSERT_TRUE(fs_manager_->CheckAndOpenFileSystemRoots().ok());
  fs_manager_->SetTabletPathByDataPath(tablet_uuid_, fs_manager_->GetDataRootDirs()[0]);
  auto status =
      ConsensusMetadata::Create(fs_manager_.get(), tablet_uuid_, peer_uuid_, config_, 0, &meta);
  if (!status.ok()) FAIL();

  table_metric_entity_ = METRIC_ENTITY_table.Instantiate(metric_registry_.get(), "test-table");
  tablet_metric_entity_ =
      METRIC_ENTITY_tablet.Instantiate(metric_registry_.get(), "test-tablet");

  rpc::MessengerBuilder builder(peer_uuid_);
  auto messenger = builder.Build();
  ASSERT_TRUE(messenger.ok());
  messenger_.reset(messenger->release());
  proxy_cache_ = std::make_unique<rpc::ProxyCache>(messenger_.get());

  init_log();

  consensus_ = RaftConsensus::Create(
      options_, std::move(meta), local_peer_pb_, table_metric_entity_, tablet_metric_entity_, clock_,
      operation_factory_.get(), messenger_.get(), proxy_cache_.get(), log_, nullptr, nullptr,
      Bind(
          &::yb::consensus::RaftInstance::TabletPeerStateChangedCallback,
          Unretained(this),
          tablet_uuid_),
      TableType::DEFAULT_TABLE_TYPE, thread_pool_.get(), 0, 0);
}

void RaftInstance::init_log() {
  init_schema();
  LogOptions options;
  ASSERT_TRUE(Log::Open(
                  options, tablet_uuid_, tablet_wal_path_, peer_uuid_, *schema_.get(), 0, nullptr,
                  nullptr, thread_pool_.get(), thread_pool_.get(), thread_pool_.get(),
                  std::numeric_limits<int64_t>::max(), &log_)
                  .ok());
}

void RaftInstance::init_schema() {
  schema_.reset(new Schema{
      {ColumnSchema("key", INT32, false, true), ColumnSchema("int_val", INT32),
       ColumnSchema("string_val", STRING, true)},
      1});
  auto schema_with_ids = SchemaBuilder(*schema_.get()).Build();
  *schema_ = schema_with_ids;
}

void RaftInstance::start_rpc_server(const server::RpcServerOptions& opts) {
  rpc_server_ = std::make_unique<server::RpcServer>(
      peer_uuid_, opts, rpc::CreateConnectionContextFactory<rpc::YBInboundConnectionContext>());
  server_metric_entity_ = METRIC_ENTITY_server.Instantiate(metric_registry_.get(), "server");
  rpc::MessengerBuilder builder("server");
  builder.set_metric_entity(server_metric_entity_);
  auto messenger = builder.Build();
  ASSERT_TRUE(messenger.ok());
  server_messenger_.reset(messenger->release());
  service_impl_.reset(new MyConsensusServiceImpl(consensus_.get(), server_metric_entity_));
  ASSERT_TRUE(rpc_server_->Init(server_messenger_.get()).ok());
  ASSERT_TRUE(rpc_server_->RegisterService(10000, std::move(service_impl_)).ok());
  ASSERT_TRUE(rpc_server_->Start().ok());
}
TEST_F(RaftTest, a) {
  fLS::FLAGS_log_dir = "/tmp";
  fLB::FLAGS_logtostderr = false;
  google::SetLogDestination(google::GLOG_INFO, "/tmp/INFO_");
  std::unique_ptr<RaftConfigPB> config(new RaftConfigPB());
  auto* peer = config->add_peers();
  peer->set_permanent_uuid("uuid-1");
  peer->set_member_type(PeerMemberType::VOTER);
  auto addr = peer->mutable_last_known_private_addr()->Add();
  addr->set_host("127.0.0.1");
  addr->set_port(9001);
  peer = config->add_peers();
  peer->set_permanent_uuid("uuid-2");
  peer->set_member_type(PeerMemberType::VOTER);
  addr = peer->mutable_last_known_private_addr()->Add();
  addr->set_host("127.0.0.2");
  addr->set_port(9002);
  peer = config->add_peers();
  peer->set_permanent_uuid("uuid-3");
  peer->set_member_type(PeerMemberType::VOTER);
  addr = peer->mutable_last_known_private_addr()->Add();
  addr->set_host("127.0.0.3");
  addr->set_port(9003);
  set_config(std::move(config));
  create_instances();
  start_instances();

  std::this_thread::sleep_for(1h);
}

}  // namespace consensus
}  // namespace yb
