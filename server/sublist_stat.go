// sthg: statistics files for monitoring enhancements
package server

import (
	"sync/atomic"
)

type subjCount struct {
	isAccount bool // 标识 account 所属 subllist
	accName   string
	subs      int64 // 主题数量
}

func (sc *subjCount) setAccountFlag(name string) {
	sc.isAccount = true
	sc.accName = name
}

// 主题数计数
func (sc *subjCount) add(subj []byte, n *node, delta int64) {
	if !sc.isAccount {
		return
	}
	// fmt.Println("subj:", string(subj))
	if !FilterSubjectPolice1(subj) {
		return
	}
	if n == nil || (len(n.qsubs) == 0 && len(n.psubs) == 0) {
		atomic.AddInt64(&sc.subs, delta)
	}
}
