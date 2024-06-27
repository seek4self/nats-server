// sthg: statistics files for monitoring enhancements
package server

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server/pse"
)

const (
	SummzPath       = "/Summz"
	SubListzPath    = "/SubListz"
	SubClientszPath = "/SubClientsz"
)

// customHandler 自定义监控路由
func (s *Server) customHandler(mux *http.ServeMux) {
	// /Summz
	mux.HandleFunc(s.basePath(SummzPath), s.HandleSummz)
	// /SubListz
	mux.HandleFunc(s.basePath(SubListzPath), s.HandleSubListz)
	// /SubClientsz
	mux.HandleFunc(s.basePath(SubClientszPath), s.HandleClientsz)
}

func (s *Server) HandleSummz(w http.ResponseWriter, r *http.Request) {

	s.mu.Lock()
	s.httpReqStats[SummzPath]++
	s.mu.Unlock()
	sums := s.newSummz()
	b, err := json.MarshalIndent(sums, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /summz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

type Summz struct {
	Conns     int    `json:"cons"`
	Subs      int64  `json:"subs"`
	Consumers int64  `json:"consumers"`
	Pubers    int64  `json:"pubers"`
	MemoryUse int64  `json:"memoryUse"`
	PubMsgs   int64  `json:"pubMsgs"`
	PubBytes  int64  `json:"pubBytes"`
	SubMsgs   int64  `json:"subMsgs"`
	SubBytes  int64  `json:"subBytes"`
	JsMsgs    uint64 `json:"jsMsgs"`
	JsUse     uint64 `json:"jsUse"`
}

// Summz return the summary stat of the server
func (s *Server) newSummz() *Summz {
	var clist map[uint64]*client = s.clients
	js, _ := s.Jsz(&JSzOptions{})

	var rss, vss int64
	var pcpu float64
	// We want to do that outside of the lock.
	pse.ProcUsage(&pcpu, &rss, &vss)

	sum := &Summz{
		Conns:     len(clist),
		JsMsgs:    js.Messages,
		PubMsgs:   atomic.LoadInt64(&s.pubMsgs),
		PubBytes:  atomic.LoadInt64(&s.pubBytes),
		SubBytes:  atomic.LoadInt64(&s.subBytes),
		SubMsgs:   atomic.LoadInt64(&s.subMsgs),
		MemoryUse: vss,
		Consumers: atomic.LoadInt64(&s.consumers),
		Pubers:    atomic.LoadInt64(&s.produsers),
		JsUse:     js.Bytes,
		Subs:      s.getClusterSubs(),
	}

	return sum
}

func (s *Server) getClusterSubs() int64 {
	var subs int64
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		subs += acc.sl.subs
		return true
	})
	return subs
}

type ListSumm struct {
	List   []*ListInfo `json:"list"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
	Total  int         `json:"total"`
	Now    time.Time   `json:"now"`
}

type SubCons struct {
	Cons   []*ConStat `json:"cons"`
	Offset int        `json:"offset"`
	Limit  int        `json:"limit"`
	Total  int        `json:"total"`
	Now    time.Time  `json:"now"`
}

type ConStat struct {
	Cid        uint64 `json:"cid"`
	ClientAddr string `json:"clientIp"`
	ServerAddr string `json:"nodeIp"`
	Language   string `json:"lang"`
	Version    string `json:"version"`
	Pubs       uint64 `json:"inMsgs"`
	Subs       uint64 `json:"outMsgs"`
	Subjects   uint64 `json:"subjects"`

	Rtt time.Duration `json:"rtt"`
	Now time.Time     `json:"lastActivity"`
}

type ListInfo struct {
	Subject string         `json:"subject"`
	Cons    int            `json:"connectionsNum"`
	Subs    uint64         `json:"outMsgs"`
	Pubs    uint64         `json:"inMsgs"`
	CidMap  map[uint64]int `json:"cidmap"`
}

type ListOptions struct {
	// Offset is used for pagination. Sublistz() only returns subs starting at this
	// offset from the global results.
	Offset int    `json:"offset"`
	Cid    uint64 `json:"cid"`
	// Limit is the maximum number of subs that should be returned by Sublistz().
	Limit   int    `json:"limit"`
	subName string `json:"subName"`
}

type SubConOption struct {
	Subject  string `json:"subject"`
	Offset   int    `json:"offset"`
	Limit    int    `json:"limit"`
	ClientIp string `json:"clientIp"`
}

type subMsgDetail struct {
	SubDetail
	Rmsgs   int64 `json:"rmsgs"`
	RMbytes int64 `json:"rmbytes"`
}

func newSubMsgDetail(sub *subscription) subMsgDetail {
	sub.client.mu.Lock()
	defer sub.client.mu.Unlock()
	return subMsgDetail{
		SubDetail: newSubDetail(sub),
		Rmsgs:     sub.rm,
		RMbytes:   sub.nb,
	}
}

func (s *Server) HandleClientsz(w http.ResponseWriter, r *http.Request) {

	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	subject := r.URL.Query().Get("subject")
	clientIp := r.URL.Query().Get("clientIp")

	subOpts := &SubConOption{
		Offset:   offset,
		Limit:    limit,
		Subject:  subject,
		ClientIp: clientIp,
	}
	s.mu.Lock()
	s.httpReqStats[SubClientszPath]++
	s.mu.Unlock()

	l, err := s.SubConsz(subOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /SubListz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

func (s *Server) SubConsz(opts *SubConOption) (*SubCons, error) {
	var (
		offset   int
		limit    int
		subj     string
		clientIp string
		cip      string
		cport    uint16
	)

	if opts != nil {
		offset = opts.Offset
		if offset < 0 {
			offset = 0
		}
		limit = opts.Limit
		if limit <= 0 {
			limit = DefaultSubListSize
		}
		clientIp = opts.ClientIp
		ips := strings.Split(clientIp, ":")
		if len(ips) == 2 {
			cip = ips[0]
			temport, err := strconv.Atoi(ips[1])
			if err == nil {
				cport = uint16(temport)
			}
		}
		subj = opts.Subject
	}

	result := &SubCons{
		Limit:  limit,
		Offset: offset,
		Now:    time.Now().UTC(),
	}
	r := make([]*ConStat, 0)
	clist := s.clients

	//subj 不为空为明细接口，subj为空表示是连接的一级不显示主题数等统计。
	if subj != "" {
		// 第一步先获取sublist
		// 第二步对sublist 去掉不需要的主题
		// 第三步需要处理clinets 处理pub和 pull主题
		var sublists []subMsgDetail = make([]subMsgDetail, 0)
		subs := s.getSubscriptions()
		// TODO(dlc) - may be inefficient and could just do normal match when total subs is large and filtering.
		for _, sub := range subs {
			// Check for filter

			if sub.client == nil {
				continue
			}
			//  此处应该过滤掉非业务的主题数据
			if !FilterSubjectPolice1(sub.subject) {
				continue
			}
			sublists = append(sublists, subMsgDetail{
				SubDetail: newSubDetail(sub),
				Rmsgs:     sub.rm,
				RMbytes:   sub.nb,
			})
		}
		for _, c := range clist {
			msgs, ok := c.subjMsgs[subj]
			//这里ok 意味这有俩种情况  pub的主题消息 或者sub pull主题消息
			subMsgs, ok1 := getSingleSubStat(subj, sublists, c.cid)
			li := newConStat(c, s)
			if ok {
				li.Pubs = msgs[0]
				li.Subs = msgs[1]
				li.Subs += uint64(subMsgs)
				r = append(r, li)
			} else if ok1 {
				li.Subs += uint64(subMsgs)
				r = append(r, li)
			}
		}
	} else if clientIp != "" {
		for _, c := range clist {
			if c.host == cip && c.port == cport {
				l := newConStat(c, s)
				r = append(r, l)
				break
			}
		}
	} else {
		for _, c := range clist {
			l := newConStat(c, s)
			r = append(r, l)
		}
	}

	minoff := result.Offset
	maxoff := result.Offset + result.Limit

	maxIndex := len(r)

	// Make sure these are sane.
	if minoff > maxIndex {
		minoff = maxIndex
	}
	if maxoff > maxIndex {
		maxoff = maxIndex
	}
	result.Total = len(r)
	result.Cons = r[minoff:maxoff]
	return result, nil

}

func getSingleSubStat(sub string, sublist []subMsgDetail, cid uint64) (uint64, bool) {
	for i, _ := range sublist {
		if sublist[i].Subject == sub && cid == sublist[i].Cid {
			return uint64(sublist[i].Rmsgs), true
		}
	}
	return 0, false
}

func newConStat(c *client, s *Server) *ConStat {
	m := &ConStat{
		Cid:        c.cid,
		Language:   c.opts.Lang,
		Version:    c.opts.Version,
		ClientAddr: net.JoinHostPort(c.host, strconv.Itoa(int(c.port))),
		ServerAddr: net.JoinHostPort(s.getHostIP(), strconv.Itoa(s.opts.Port)),
		Pubs:       0,
		Subs:       0,
		Now:        time.Now().UTC(),
		Rtt:        c.rtt,
	}
	return m
}

func (s *Server) getHostIP() string {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, _ := netInterfaces[i].Addrs()
		for _, address := range addrs {
			ipnet, ok := address.(*net.IPNet)
			if ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func (s *Server) getSubscriptions() []*subscription {
	var raw [4096]*subscription
	subs := raw[:0]
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		acc.sl.localSubs(&subs, false)
		return true
	})
	return subs
}

func (s *Server) HandleSubListz(w http.ResponseWriter, r *http.Request) {
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	cid := uint64(0)
	cidstr := r.URL.Query().Get("cid")
	subName := r.URL.Query().Get("subName")
	scid, err := strconv.ParseUint(cidstr, 10, 64)
	if err == nil {
		cid = scid
	}

	listOpts := &ListOptions{
		Offset:  offset,
		Limit:   limit,
		Cid:     uint64(cid),
		subName: subName,
	}
	s.mu.Lock()
	s.httpReqStats[SubListzPath]++
	s.mu.Unlock()
	l, err := s.Sublistz(listOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /SubListz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)

}

func (s *Server) Sublistz(opts *ListOptions) (*ListSumm, error) {
	var (
		offset  int
		limit   = DefaultConnListSize
		cid     uint64
		subName string
	)

	if opts != nil {
		offset = opts.Offset
		if offset <= 0 {
			offset = 0
		}

		if opts.Limit > 0 {
			limit = opts.Limit
		}
		cid = opts.Cid
		subName = opts.subName

	}
	l := &ListSumm{
		Offset: offset,
		Limit:  limit,
		Now:    time.Now().UTC(),
	}
	clist := s.clients

	sublistMap := make(map[string]*ListInfo)

	// 第一步先获取sublist
	// 第二步对sublist 去掉不需要的主题
	// 第三步需要处理clinets 处理pub和 pull主题
	subs := s.getSubscriptions()
	// TODO(dlc) - may be inefficient and could just do normal match when total subs is large and filtering.
	for _, sub := range subs {
		// Check for filter

		if sub.client == nil || sub.client.kind != CLIENT {
			continue
		}

		//  此处应该过滤掉非业务的主题数据
		if !FilterSubjectPolice1(sub.subject) {
			continue
		}
		// 有主题名称匹配
		if subName != "" && subName != string(sub.subject) {
			continue
		}
		if cid > 0 && cid != sub.client.cid {
			continue
		}
		newSublistMap(sublistMap, newSubMsgDetail(sub))
		if subName != "" {
			break
		}
	}

	// 开始clist的遍历,pub的统计还有 pull消费的统计
	for i, _ := range clist {
		subClientWithCid(cid, clist, i, subName, sublistMap)
	}

	sublist := make([]*ListInfo, 0)

	for _, v := range sublistMap {
		sublist = append(sublist, v)
	}

	minoff := l.Offset
	maxoff := l.Offset + l.Limit

	maxIndex := len(sublist)
	l.Total = len(sublist)
	// Make sure these are sane.
	if minoff > maxIndex {
		minoff = maxIndex
	}
	if maxoff > maxIndex {
		maxoff = maxIndex
	}
	l.List = sublist[minoff:maxoff]
	return l, nil
}

func subClientWithCid(cid uint64, clist map[uint64]*client, i uint64, subName string, sublistMap map[string]*ListInfo) {
	var filterCid bool = true
	if cid == 0 {
		// cid参数 为空时，使用当前客户端连接的 cid
		cid = clist[i].cid
		filterCid = false
	}
	if cid != clist[i].cid {
		return // cid 参数与当前遍历客户端 cid 不匹配，结束
	}
	// 这里表示传递了具体的主题名称
	for subj, msgs := range clist[i].subjMsgs {
		if subName != "" && subName != subj {
			continue // 传入主题不匹配，遍历下一条
		}
		// 主题参数为空 或者 主题匹配
		newClientSubMap(sublistMap, subj, msgs, cid)
		if filterCid && subName != "" {
			break // 找到对应 cid 和 主题 subj, 结束遍历
		}
	}
}

func newClientSubMap(sublistMap map[string]*ListInfo, subj string, msgs [2]uint64, cid uint64) {
	if li, ok := sublistMap[subj]; ok {
		li.Subs += msgs[1]
		li.Pubs += msgs[0]
		if civ, ok := li.CidMap[cid]; ok {
			li.CidMap[cid] = civ + 1
		} else {
			li.CidMap[cid] = 1
			li.Cons++
		}
		sublistMap[subj] = li
		return
	}

	//no match
	li := &ListInfo{
		Subject: subj,
		Subs:    msgs[1],
		Pubs:    msgs[0],
		Cons:    1,
		CidMap:  make(map[uint64]int),
	}
	li.CidMap[cid] = 1
	sublistMap[subj] = li

}

func newSublistMap(sublistMap map[string]*ListInfo, subdetail subMsgDetail) {
	if v, ok := sublistMap[subdetail.Subject]; ok {
		v.Subs += uint64(subdetail.Rmsgs)
		if _, cidok := v.CidMap[subdetail.Cid]; !cidok {
			v.CidMap[subdetail.Cid] = 1
			v.Cons++
		}
		return
	}

	li := &ListInfo{
		Subject: subdetail.Subject,
		Subs:    uint64(subdetail.Rmsgs),
		Pubs:    0,
		Cons:    1,
		CidMap:  make(map[uint64]int),
	}
	li.CidMap[subdetail.Cid] = 1
	sublistMap[subdetail.Subject] = li
}
