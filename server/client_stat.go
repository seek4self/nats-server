// sthg: statistics files for monitoring enhancements
package server

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// FilterSubjectPolice1 过滤系统主题
func FilterSubjectPolice1(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	// fmt.Println("subj", string(raw))
	return !isInternalSubj(raw)
}

const (
	op            int = iota
	op_Doller         // $
	op_Doller_J       // $J
	op_Doller_JS      // $JS
	op_Doller_JSC     // $JSC
	op_Doller_S       // $S
	op_Doller_SY      // $SY
	op_Doller_SYS     // $SYS
	op_Doller_N       // $N
	op_Doller_NR      // $NR
	op_Doller_NRG     // $NRG
	op_Line           // _
	op_Line_I         // _I
	op_Line_IN        // _IN
	op_Line_INB       // _INB
	op_Line_INBO      // _INBO
	op_Line_INBOX     // _INBOX
	op_Line_R         // _R
	op_Line_R_        // _R_
)

// isInternalSubj 检查系统内部主题
func isInternalSubj(raw []byte) bool {
	var (
		b     byte
		state = op
	)
	for i := 0; i < len(raw); i++ {
		b = raw[i]
		switch state {
		case op:
			if b == '$' {
				state = op_Doller
			} else if b == '_' {
				state = op_Line
			} else {
				return false
			}
		case op_Doller:
			switch b {
			case 'J':
				state = op_Doller_J
			case 'S':
				state = op_Doller_S
			case 'N':
				state = op_Doller_N
			default:
				return false
			}
		case op_Doller_J:
			if b != 'S' {
				return false
			}
			state = op_Doller_JS
		case op_Doller_JS:
			if b != 'C' {
				return b == '.'
			}
			state = op_Doller_JSC
		case op_Doller_JSC, op_Doller_SYS, op_Doller_NRG, op_Line_INBOX, op_Line_R_:
			return b == '.'
		case op_Doller_S:
			if b != 'Y' {
				return false
			}
			state = op_Doller_SY
		case op_Doller_SY:
			if b != 'S' {
				return false
			}
			state = op_Doller_SYS
		case op_Doller_N:
			if b != 'R' {
				return false
			}
			state = op_Doller_NR
		case op_Doller_NR:
			if b != 'G' {
				return false
			}
			state = op_Doller_NRG
		case op_Line:
			if b == 'I' {
				state = op_Line_I
			} else if b == 'R' {
				state = op_Line_R
			} else {
				return false
			}
		case op_Line_I:
			if b != 'N' {
				return false
			}
			state = op_Line_IN
		case op_Line_IN:
			if b != 'B' {
				return false
			}
			state = op_Line_INB
		case op_Line_INB:
			if b != 'O' {
				return false
			}
			state = op_Line_INBO
		case op_Line_INBO:
			if b != 'X' {
				return false
			}
			state = op_Line_INBOX
		case op_Line_R:
			if b != '_' {
				return false
			}
			state = op_Line_R_
		default:
			return false
		}
	}
	return false
}

func checkInbox(raw []byte) bool {
	if len(raw) > 6 &&
		raw[0] == '_' &&
		raw[1] == 'I' &&
		raw[2] == 'N' &&
		raw[3] == 'B' &&
		raw[4] == 'O' &&
		raw[5] == 'X' {
		return true
	}
	return false
}

// isHeartbeatMsg 检查 push 消费者心跳消息
func isHeartbeatMsg(raw []byte) bool {
	idleMsgHeader := "NATS/1.0 100 Idle Heartbeat"
	if len(raw) < len(idleMsgHeader) {
		return false
	}
	if raw[13] == 'I' && raw[14] == 'd' && raw[15] == 'l' && raw[16] == 'e' {
		return true
	}
	return false
}

type conKind int

const (
	conKindNone conKind = iota
	conKindPub
	conKindSub
)

// 主题收发消息计数
type subjMsgsCount struct {
	// 主题消息数量统计, 0: 发布主题； 1:消费主题
	subjMsgs map[string][2]uint64
	// 客户端类型为生产者
	statPub bool
	// 客户端类型为消费者
	statSub bool
	conKind
	subMap map[string]int
}

func (mc *subjMsgsCount) setConnKind(k conKind) {
	mc.conKind = k
}

func (mc *subjMsgsCount) init() {
	mc.subjMsgs = make(map[string][2]uint64)
}

// add 主题数量计数累加
func (mc *subjMsgsCount) add(subject []byte, kind conKind) {
	subj := string(subject)
	if v, ok := mc.subjMsgs[subj]; ok {
		v[kind-1]++
		mc.subjMsgs[subj] = v
		return
	}
	if kind == conKindPub {
		mc.subjMsgs[subj] = [2]uint64{1, 0}
	} else {
		mc.subjMsgs[subj] = [2]uint64{0, 1}
	}
}

// msgCount 订阅消息计数
type msgCount struct {
	rm int64 //real messages
	nb int64 //This is to stat Messages total bytes
}

// add 订阅消息计数累加
func (mc *msgCount) add(size int) {
	mc.rm++
	mc.nb += int64(size)
}

// clean 订阅消息计数清零
func (mc *msgCount) clean() {
	mc.rm, mc.nb = 0, 0
}

type summaryStats struct {
	pubMsgs      int64
	subMsgs      int64
	pubBytes     int64
	subBytes     int64
	subjectCount int64
	subjectMap   map[string]int
	consumerMap  sync.Map
	puberMap     sync.Map
	produsers    int64
	consumers    int64
}

// 暂时无用
func (ss *summaryStats) initSummary() {
	// ss.subjectMap = make(map[string]int)
	// ss.puberMap = sync.Map{}
	// ss.consumerMap = sync.Map{}
}

// SubAdd 订阅消息计数累加
func (ss *summaryStats) SubAdd(size int) {
	atomic.AddInt64(&ss.subMsgs, 1)
	atomic.AddInt64(&ss.subBytes, int64(size))
}

// PubAdd 发布消息计数累加
func (ss *summaryStats) PubAdd(size int) {
	atomic.AddInt64(&ss.pubMsgs, 1)
	atomic.AddInt64(&ss.pubBytes, int64(size))
}

// statPubMsg 统计生产消息和生产者数量
func (c *client) statPubMsg() {
	// fmt.Printf("pub subj: %s, reply: %s\n", string(c.pa.subject), string(c.pa.reply))
	if c.kind != CLIENT {
		return
	}
	if !FilterSubjectPolice1(c.pa.subject) {
		return
	}
	c.srv.PubAdd(c.pa.size)
	if !c.statPub {
		atomic.AddInt64(&c.srv.produsers, 1)
		c.statPub = true
	}
	c.mu.Lock()
	c.subjMsgsCount.add(c.pa.subject, conKindPub)
	c.mu.Unlock()
}

// statConsumer 统计消费者数量
func (c *client) statConsumer(subject []byte) {
	if c.kind != CLIENT {
		return
	}
	if c.statSub {
		return // 提前返回，省去检查主题的步骤
	}
	if !FilterSubjectPolice1(subject) {
		return
	}
	atomic.AddInt64(&c.srv.consumers, 1)
	c.statSub = true
}

// statSubMsg 统计订阅消息数和消费者数量
func (c *client) statSubMsg(sub *subscription, msg []byte) {
	if c.kind != CLIENT {
		return
	}
	msgSize := len(msg) - LEN_CR_LF
	// fmt.Println("sub msg：", string(msg), "subj:", string(sub.subject), "deliver:", string(sub.deliver))
	if FilterSubjectPolice1(sub.subject) && !isHeartbeatMsg(msg) {
		c.srv.SubAdd(msgSize)
		sub.msgCount.add(msgSize)
	}
	// 这里主要是处理 jetstream pull消费者的问题
	// pull 消费者主题为 _INBOX.xxx， 实际主题为 sub.deliver
	if len(sub.deliver) == 0 || !checkInbox(sub.subject) {
		return
	}
	c.srv.SubAdd(msgSize)
	c.subjMsgsCount.add(sub.deliver, conKindSub)
	if c.statSub {
		return
	}
	atomic.AddInt64(&c.srv.consumers, 1)
	c.statSub = true
}

// cleanPubSub 消费者或生产者计数减一
func (c *client) cleanPubSub() {
	if c.statPub {
		atomic.AddInt64(&c.srv.produsers, -1)
		c.statPub = false
	}
	if c.statSub {
		atomic.AddInt64(&c.srv.consumers, -1)
		c.statSub = false
	}
}

// handleDeliverSubject 处理 pull 消费者主题
func (c *client) handleDeliverSubject(r *SublistResult, acc *Account, msg []byte) bool {
	// fmt.Printf("route subj: %s, reply: %s, msg: %s\n", string(c.pa.subject), string(c.pa.reply), string(msg))
	if len(r.psubs)+len(r.qsubs) == 0 {
		return false
	}
	if !checkInbox(c.pa.subject) {
		return false
	}

	li := bytes.LastIndexByte(c.pa.reply, '@')
	if li == -1 || li == len(c.pa.reply)-1 {
		return false
	}
	deliver := c.pa.reply[li+1:]
	c.processMsgResults(acc, r, msg, deliver, c.pa.subject, c.pa.reply, pmrNoFlag)
	return true
}
