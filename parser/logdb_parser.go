package parser

import (
	"github.com/qiniu/log.v1"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/conf"
)

// 一个自定义parser的示例，将日志放到data中的log字段中
type LogdbParser struct {
	name string
}

func NewLogdbParser(c conf.MapConf) (parser.LogParser, error) {
	name, _ := c.GetString(KeyParserName)
	// 获取parser配置中的name项，默认myparser
	p := &LogdbParser{
		name: name,
	}
	return p, nil
}

func (p *LogdbParser) Name() string {
	return p.name
}

//appid,repo,respCode,method,reqBodyLength
func (p *LogdbParser) Parse(lines []string) (datas []sender.Data, err error) {
	aggRet := make(map[logDBAggKey]int64, 0)
	ret := []sender.Data{}
	se := &utils.StatsError{}
	for _, line := range lines {
		row, parseErr := ParselogToRow(line)
		if parseErr != nil {
			se.AddErrors()
			se.ErrorDetail = parseErr
			// if cannot parse, log and omit
			log.Errorf("Cannot parse line to Row : %v , error is %v", line, parseErr)
			continue
		}
		se.AddSuccess()
		key := logDBAggKey{
			appid:    row.ReqAppID(),
			repo:     row.ReqRepo(),
			time5Min: Time5MinInt(row.ReqTime()),
		}
		flow := row.CalReqLength()
		_, exist := aggRet[key]
		if exist {
			aggRet[key] += flow
		} else {
			aggRet[key] = flow
		}
	}
	for key, value := range aggRet {
		data := sender.Data{}
		data["appid"] = key.appid
		data["repo"] = key.repo
		data["time5Min"] = key.time5Min
		data["flow"] = value
		ret = append(ret, data)
	}
	return ret, se
}

type logEntry struct {
}
type logDBAggKey struct {
	appid    string
	repo     string
	time5Min int64
}
