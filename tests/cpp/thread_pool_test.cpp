#include <gtest/gtest.h>
#include <taskforge/thread_pool.hpp>
#include <taskforge/runtime.hpp>
#include <atomic>
#include <future>
#include <latch>
TEST(ThreadPool, RejectsEmptyPool){EXPECT_THROW(taskforge::ThreadPool pool(0),std::invalid_argument);}
TEST(ThreadPool, ExecutesAllSubmittedTasks){
 std::atomic<int> executed=0;std::latch complete(4);
 taskforge::ThreadPool pool(2);
 for(int i=0;i<4;i++)ASSERT_TRUE(pool.submit([&](std::stop_token){executed++;complete.count_down();}));
 complete.wait();EXPECT_EQ(executed.load(),4);pool.shutdown();
 EXPECT_FALSE(pool.submit([](std::stop_token){}));
}
TEST(ThreadPool, BoundsPendingQueueAndInterruptsLanes){
 std::latch entered(2);std::atomic<int> stopped=0;taskforge::ThreadPool pool(2);
 for(int i=0;i<2;i++)ASSERT_TRUE(pool.submit([&](std::stop_token stop){entered.count_down();taskforge::pause(stop,std::chrono::seconds(30));if(stop.stop_requested())stopped++;}));
 entered.wait();
 for(int i=0;i<4;i++)EXPECT_TRUE(pool.submit([](std::stop_token){}));
 EXPECT_FALSE(pool.submit([](std::stop_token){}));
 const auto start=std::chrono::steady_clock::now();pool.shutdown();
 EXPECT_EQ(stopped.load(),2);EXPECT_LT(std::chrono::steady_clock::now()-start,std::chrono::seconds(2));
}
