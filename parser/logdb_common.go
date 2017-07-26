package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qiniu/log"
	"net/url"
	"strconv"
	"strings"
)

const SECOND_PER_5MIN = 5 * 60

func Time5MinInt(t int64) int64 {
	return (t / SECOND_PER_5MIN) * SECOND_PER_5MIN
}

var ErrInValidFieldCnt = errors.New("invalid field count")

const (
	TAG_SERVICE         = 1
	TAG_TIME            = 2
	TAG_METHOD          = 3
	TAG_HEADER          = 5
	TAG_RESPONSE_HEADER = 8
	TAG_RESQUEST_LENGTH = 10
)

func ParselogToRow(line string) (*Row, error) {
	idx := strings.Index(line, "REQ")
	if idx < 0 {
		return nil, errors.New("invalid reqlog")
	}
	line = line[idx:]
	a := new(Row)
	a.data = strings.Split(line, "\t")
	if len(a.data) != 12 && len(a.data) != 14 {
		return nil, ErrInValidFieldCnt
	}
	if a.data[0] != "REQ" {
		return nil, fmt.Errorf("invalid Head: %v", a.data[0])
	}
	return a, nil
}

/* audit log example
REQ     LogService      14773877470553448       GET     /v5/repos/qcos_nq_pod_2147832099/search {"Accept-Encoding":"gzip","Host":"logdb.qiniu.com","IP":"127.0.0.1","RawQuery":"from=0\u0026q=collectedAt%3A%5B2016-10-25T00%3A00%3A00%2B08%3A00+TO+2016-10-25T16%3A17%3A42%2B08%3A00%5D+AND+podName%3Adefault%252Fhello\u0026size=50\u0026sort=collectedAt%3Aasc","Start-time":1477387747055,"X-Forwarded-For":"180.168.57.238, 192.168.33.42, 192.168.33.42","X-Real-Ip":"180.168.57.238","X-Reqid":"Jz2owFFl4yUPWBIA","X-Scheme":"https"}            200     {"Content-Length":"4362","Content-Type":"application/json","End-time":1477387747154,"X-Appid":"1380793874","X-Log":["LogService:10"],"X-Operation":"V5_SearchData","X-Repo":"qcos_nq_pod_2147832099","X-Reqid":"Jz2owFFl4yUPWBIA"}              4362    991596
REQ	LogService	15000182611599302	POST	/v5/repos/kodo_z0_req_ebdmaster/data	{"Accept-Encoding":"gzip","Content-Type":"application/json","Host":"192.168.76.51:13265","IP":"192.168.76.51","RawQuery":"error=true","Start-time":1500018261154,"X-Forwarded-For":"192.168.82.26","X-Reqid":"gm8AAOWGuzupItEU","X-Source":"VIP_iVYhbapFikbuZx62_0000000000","bs":1486821}		200	{"Content-Length":"51","Content-Type":"application/json","End-time":1500018262422,"X-Appid":"1380921591","X-Data-Cluster":"qiniues","X-Data-Flowtype":"es","X-Fail":"0","X-Log":["ioRead:33;UnmarshalToArrryRawmessage:1;elasticBulkReq:1163;LogService:1262"],"X-Operation":"V5_PostData","X-Repo":"kodo_z0_req_ebdmaster","X-Repo-Retention":"7","X-Req-Bodylength":"1486821","X-Reqid":"gm8AAOWGuzupItEU","X-Success":"1950","X-Total":"1950","hostName":"nb72"}	{"success":1950,"failed":0,"total":1950,"items":[]}	51	12622979
*/
type Row struct {
	data []string
	// 以下三个字段延迟加载，请使用get方法获得
	reqHeader  *ReqHeader
	respHeader *RespHeader
	rawQuery   *url.Values
}

type ReqHeader struct {
	ContentLength string `json:"Content-Length"`
}

type RespHeader struct {
	XRepo  string `json:"X-Repo"`
	XAppID string `json:"X-Appid"`
}

func (a *Row) ReqLength() (reqLength int64) {
	reqHeader := a.getReqHeader()
	if reqHeader == nil {
		return
	}
	reqLength, _ = strconv.ParseInt(reqHeader.ContentLength, 10, 64)
	if reqLength > 0 {
		return reqLength
	}
	return 0
}

func (a *Row) CalReqLength() (reqLength int64) {
	requestContentLength := a.data[TAG_RESQUEST_LENGTH]
	if requestContentLength == "" {
		return
	}
	reqLength, _ = strconv.ParseInt(requestContentLength, 10, 64)
	if reqLength > 0 {
		return reqLength
	}
	return 0
}

// get unix second of time
func (a *Row) ReqTime() int64 {
	t, err := strconv.ParseInt(a.data[TAG_TIME], 10, 0)
	if err != nil {
		log.Errorf("cannot parse time %v to int", a.data[TAG_TIME])
	}
	return t / 10000000
}

func (a *Row) Service() string {
	return a.data[TAG_SERVICE]
}

func (a *Row) Method() string {
	return a.data[TAG_METHOD]
}

func (a *Row) ReqAppID() string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return ""
	}
	return respHeader.XAppID
}

func (a *Row) ReqRepo() string {
	respHeader := a.getRespHeader()
	if respHeader == nil {
		return ""
	}
	return respHeader.XRepo
}

func (a *Row) getReqHeader() *ReqHeader {
	if a.reqHeader != nil {
		return a.reqHeader
	}
	err := json.Unmarshal([]byte(a.data[TAG_HEADER]), &a.reqHeader)
	if err != nil {
		log.Warnf("%q, Unmarshal ReqHeader err %v", a.data[TAG_HEADER], err)
		return nil
	}
	return a.reqHeader
}

func (a *Row) getRespHeader() *RespHeader {
	if a.respHeader != nil {
		return a.respHeader
	}
	err := json.Unmarshal([]byte(a.data[TAG_RESPONSE_HEADER]), &a.respHeader)
	if err != nil {
		log.Warnf("%q, Unmarshal RespHeader err %v", a.data[TAG_RESPONSE_HEADER], err)
		return nil
	}
	return a.respHeader
}
