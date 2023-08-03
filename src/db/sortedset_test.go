package db

import (
	"math/rand"
	"my-godis/src/redis/reply/asserts"
	"strconv"
	"testing"
)

func TestZAdd(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100

	// add new members
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]float64, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(rand.Int()), 10)
		scores[i] = rand.Float64()
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result := ZAdd(testDB, toArgs(setArgs...))
	asserts.AssertIntReply(t, result, size)

	// test zscore and zrank
	for i, member := range members {
		result := ZScore(testDB, toArgs(key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		asserts.AssertBulkReply(t, result, score)
	}

	// test zcard
	result = ZCard(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, size)

	// update members
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		scores[i] = rand.Float64() + 100
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result = ZAdd(testDB, toArgs(setArgs...))
	asserts.AssertIntReply(t, result, 0) // return number of new members

	// test updated score
	for i, member := range members {
		result := ZScore(testDB, toArgs(key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		asserts.AssertBulkReply(t, result, score)
	}
}

func TestZRank(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(rand.Int()), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	ZAdd(testDB, toArgs(setArgs...))

	// test  zrank
	for i, member := range members {
		result := ZRank(testDB, toArgs(key, member))
		asserts.AssertIntReply(t, result, i)

		result = ZRevRank(testDB, toArgs(key, member))
		asserts.AssertIntReply(t, result, size-i-1)
	}
}

func TestZRange(t *testing.T) {
	// prepare
	FlushAll(testDB, [][]byte{})
	size := 100
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(rand.Int()), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	result := ZAdd(testDB, toArgs(setArgs...))
	reverseMembers := make([]string, size)
	for i, v := range members {
		reverseMembers[size-i-1] = v
	}

	start := "0"
	end := "9"
	result = ZRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:10])
	result = ZRevRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:10])

	start = "0"
	end = "200"
	result = ZRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, members)
	result = ZRevRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers)

	start = "0"
	end = "-10"
	result = ZRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:size-10+1])
	result = ZRevRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:size-10+1])

	start = "0"
	end = "-200"
	result = ZRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[0:0])
	result = ZRevRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[0:0])

	start = "-10"
	end = "-1"
	result = ZRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, members[90:])
	result = ZRevRange(testDB, toArgs(key, start, end))
	asserts.AssertMultiBulkReply(t, result, reverseMembers[90:])
}

func reverse(src []string) []string {
	result := make([]string, len(src))
	for i, v := range src {
		result[len(src)-i-1] = v
	}
	return result
}

func TestZRangeByScore(t *testing.T) {
	// prepare
	FlushAll(testDB, [][]byte{})
	size := 100
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	result := ZAdd(testDB, toArgs(setArgs...))

	min := "20"
	max := "30"
	result = ZRangeByScore(testDB, toArgs(key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[20:31])
	result = ZRevRangeByScore(testDB, toArgs(key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[20:31]))

	min = "-10"
	max = "10"
	result = ZRangeByScore(testDB, toArgs(key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[0:11])
	result = ZRevRangeByScore(testDB, toArgs(key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[0:11]))

	min = "90"
	max = "110"
	result = ZRangeByScore(testDB, toArgs(key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[90:])
	result = ZRevRangeByScore(testDB, toArgs(key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[90:]))

	min = "(20"
	max = "(30"
	result = ZRangeByScore(testDB, toArgs(key, min, max))
	asserts.AssertMultiBulkReply(t, result, members[21:30])
	result = ZRevRangeByScore(testDB, toArgs(key, max, min))
	asserts.AssertMultiBulkReply(t, result, reverse(members[21:30]))

	min = "20"
	max = "40"
	result = ZRangeByScore(testDB, toArgs(key, min, max, "LIMIT", "5", "5"))
	asserts.AssertMultiBulkReply(t, result, members[25:30])
	result = ZRevRangeByScore(testDB, toArgs(key, max, min, "LIMIT", "5", "5"))
	asserts.AssertMultiBulkReply(t, result, reverse(members[31:36]))
}

func TestZRem(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	size := 100
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	ZAdd(testDB, toArgs(setArgs...))

	args := []string{key}
	args = append(args, members[0:10]...)
	result := ZRem(testDB, toArgs(args...))
	asserts.AssertIntReply(t, result, 10)
	result = ZCard(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, size-10)

	// test ZRemRangeByRank
	FlushAll(testDB, [][]byte{})
	size = 100
	key = strconv.FormatInt(int64(rand.Int()), 10)
	members = make([]string, size)
	scores = make([]int, size)
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	ZAdd(testDB, toArgs(setArgs...))

	result = ZRemRangeByRank(testDB, toArgs(key, "0", "9"))
	asserts.AssertIntReply(t, result, 10)
	result = ZCard(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, size-10)

	// test ZRemRangeByScore
	FlushAll(testDB, [][]byte{})
	size = 100
	key = strconv.FormatInt(int64(rand.Int()), 10)
	members = make([]string, size)
	scores = make([]int, size)
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	ZAdd(testDB, toArgs(setArgs...))

	result = ZRemRangeByScore(testDB, toArgs(key, "0", "9"))
	asserts.AssertIntReply(t, result, 10)
	result = ZCard(testDB, toArgs(key))
	asserts.AssertIntReply(t, result, size-10)
}

func TestZCount(t *testing.T) {
	// prepare
	FlushAll(testDB, [][]byte{})
	size := 100
	key := strconv.FormatInt(int64(rand.Int()), 10)
	members := make([]string, size)
	scores := make([]int, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = strconv.FormatInt(int64(i), 10)
		scores[i] = i
		setArgs = append(setArgs, strconv.FormatInt(int64(scores[i]), 10), members[i])
	}
	result := ZAdd(testDB, toArgs(setArgs...))

	min := "20"
	max := "30"
	result = ZCount(testDB, toArgs(key, min, max))
	asserts.AssertIntReply(t, result, 11)

	min = "-10"
	max = "10"
	result = ZCount(testDB, toArgs(key, min, max))
	asserts.AssertIntReply(t, result, 11)

	min = "90"
	max = "110"
	result = ZCount(testDB, toArgs(key, min, max))
	asserts.AssertIntReply(t, result, 10)

	min = "(20"
	max = "(30"
	result = ZCount(testDB, toArgs(key, min, max))
	asserts.AssertIntReply(t, result, 9)
}

func TestZIncrBy(t *testing.T) {
	FlushAll(testDB, [][]byte{})
	key := strconv.FormatInt(int64(rand.Int()), 10)
	ZAdd(testDB, toArgs(key, "10", "a"))
	result := ZIncrBy(testDB, toArgs(key, "10", "a"))
	asserts.AssertBulkReply(t, result, "20")

	result = ZScore(testDB, toArgs(key, "a"))
	asserts.AssertBulkReply(t, result, "20")
}
