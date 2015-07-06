package storage

import (
	"fmt"
	"net/url"
	"bytes"
	"strconv"
	"crypto/rand"
	"strings"
	"time"
)
type Table struct{
	accountName string
	accountKey string
	tableName string
	intilized bool
}
func CreateTable(accountName string,accountKey string,tableName string) (Table,error){	
	t:=Table{accountName,accountKey,tableName,false}
	return t,nil;
}

const(
	headStr = `--batch_#BATCH#
Content-Type: multipart/mixed; boundary=changeset_#CHANGESET#
`

	dataStr = `
--changeset_#CHANGESET#
Content-Type: application/http
Content-Transfer-Encoding: binary

POST #TABLEURI# HTTP/1.1
Content-Type: application/json
Accept: application/json;odata=minimalmetadata
Prefer: return-no-content
DataServiceVersion: 3.0;

#JSONDATA#
`

	footStr = `
--changeset_#CHANGESET#--
--batch_#BATCH#`

)

func format(tableuri string,batch_id string,rows [][]byte)(datas string){
	set_id:=pseudo_uuid()
	datas=headStr
	for _,i:=range(rows){
		data:=strings.Replace(dataStr,"#CHANGESET#",string(set_id),-1)
		data=strings.Replace(data,"#JSONDATA#",string(i),-1)
		data=strings.Replace(data,"#TABLEURI#",tableuri,-1)
		datas+=data
	}	
	datas+=footStr
	datas=strings.Replace(datas,"#CHANGESET#",string(set_id),-1)
	datas=strings.Replace(datas,"#BATCH#",string(batch_id),-1)
	return 
}

func pseudo_uuid() (uuid string) {
    b := make([]byte, 16)
    _, err := rand.Read(b)
    if err != nil {
        fmt.Println("Error: ", err)
        return
    }
    uuid = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	uuid = strings.ToLower(uuid)
    return
}


func dumpResponse(resp *storageResponse)string{
	var r []byte=make([]byte,1024);
	resp.body.Read(r)
	return string(r)
}

func (t Table)Insert(data [][]byte)(string,error){
	
	if !t.intilized{
		r,err:=t.CreateTable(20)
		
		if err!=nil{
			fmt.Println("create table return:"+r+err.Error())
			return r,err
		}
		t.intilized=true
	}
	var client,_ = NewTableClient(t.accountName ,t.accountKey)
	uri:=client.getEndpoint("table", "$batch", url.Values{})
	headers := client.getStandardHeaders()
	batch_id:=pseudo_uuid()
	b:=[]byte(format("https://"+t.accountName+".table.core.windows.net/"+t.tableName,batch_id,data))
	headers["Content-Length"] = strconv.Itoa(len(b))
	headers["DataServiceVersion"]="3.0;"
	headers["MaxDataServiceVersion"]="3.0;NetFx"
	headers["Content-Type"] = "multipart/mixed; boundary=batch_"+batch_id
	headers["Prefer"]="return-no-content"
	resp,err := client.exec("POST", uri,headers, bytes.NewReader(b))
	return dumpResponse(resp),err

}

func (t Table)CreateTable(retry int)(string,error){

	var client,_ = NewTableClient(t.accountName ,t.accountKey )
	uri:=client.getEndpoint("table", "Tables", url.Values{})
	headers := client.getStandardHeaders()
	b:=[]byte(fmt.Sprintf(`{"TableName":"%s"}`,t.tableName))
	headers["DataServiceVersion"]="3.0;"
	headers["MaxDataServiceVersion"]="3.0;NetFx"
	headers["Content-Length"] = strconv.Itoa(len(b))
	headers["Content-Type"] = "application/json"
	headers["Prefer"]="return-no-content"
	resp,err := client.exec("POST", uri,headers, bytes.NewReader(b))
	r :=  dumpResponse(resp)
	if strings.Contains(r,"TableAlreadyExists") {
		return "TableAlreadyExists",nil
	}
	if strings.Contains(r,"TableBeingDeleted") && retry>0 {
		time.Sleep(time.Second*30)
		retry-=1
		return t.CreateTable(retry)
	}
	return dumpResponse(resp),err
}
