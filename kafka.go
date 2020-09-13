/*
 * Kafka操作封装
 * @author yorkershi
 * @created 2020-2-28
 */
package golib

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"time"
)

type Kafka struct {
	address   string //如 127.0.0.1:9092
	network   string
	topic     string
	partition int

	writeTimeout time.Duration
	readTimeout  time.Duration
	Conn         *kafka.Conn
}

//实例化Kafka操作类
func NewKafka(topic string, partition int) (*Kafka, error) {
	address := "127.0.0.1:9002"
	network := "tcp"
	conn, err := kafka.DialLeader(context.Background(), network, address, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("[KafkaConn]%s", err.Error())
	}

	return &Kafka{
		address:   address,
		network:   network,
		topic:     topic,
		partition: partition,

		writeTimeout: 10 * time.Second,
		readTimeout:  20 * time.Second,
		Conn:         conn,
	}, nil
}

//关闭kafka连接
func (this *Kafka) Close() {
	if this.Conn != nil {
		if err := this.Conn.Close(); err != nil {
			log.Printf("[ERROR][KafkaClose]%s\n", err.Error())
		}
	}
}

//设置从某个时间开始的偏移量
func (this *Kafka) SetReadOffsetByTime(tm time.Time) error {
	offset, err := this.Conn.ReadOffset(tm)
	if err != nil {
		return this.errwrap(err)
	}
	if _, err := this.Conn.Seek(offset, kafka.SeekStart); err != nil {
		return this.errwrap(err)
	} else {
		return nil
	}
}

//设置从起点位置开始的偏移量
func (this *Kafka) SetReadOffset(offset int64) error {
	if _, err := this.Conn.Seek(offset, kafka.SeekStart); err != nil {
		return this.errwrap(err)
	} else {
		return nil
	}
}

//从当前位置开始读取一条记录
func (this *Kafka) ReadMessage() ([]byte, error) {
	message, err := this.Conn.ReadMessage(64 * 1024)
	if err != nil {
		return nil, err
	}
	return message.Value, nil
}

//从当前位置开始批量读取数据
func (this *Kafka) ReadMessages() ([][]byte, error) {

	//fmt.Println(this.Conn.ReadFirstOffset())
	//fmt.Println(this.Conn.ReadLastOffset())
	//fmt.Println(this.Conn.ReadOffset(time.Now().Add(-20 * time.Minute)))
	//this.Conn.Seek(46, kafka.SeekStart)
	//fmt.Println(this.Conn.Offset())
	//fmt.Println(this.Conn.ReadBatch())

	retval := make([][]byte, 0)

	//read timeout
	if err := this.Conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return nil, this.errwrap(err)
	}

	// fetch 64B min, 1MB max
	batch := this.Conn.ReadBatch(64, 1024*1024*1)

	for {
		msg, err := batch.ReadMessage()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		retval = append(retval, msg.Value)
	}
	if err := batch.Close(); err != nil {
		log.Printf("[kafka][batch.Close]%s", err.Error())
	}
	return retval, nil
}

//将一组消息写入到kafka的某一个topic中的某一个partition中, 完成写入后连接关闭,
//返回完成写入的字节数
func (this *Kafka) WriteMessages(msglist [][]byte) (int, error) {
	if len(msglist) == 0 {
		return 0, nil
	}

	//组装消息结构体
	messages := make([]kafka.Message, len(msglist))
	for i, line := range msglist {
		messages[i] = kafka.Message{
			Value: line,
			Time:  time.Now(),
		}
	}
	//设置写入超时时间, 每次写入之前, 一定要更新写入超时时间
	if err := this.Conn.SetWriteDeadline(time.Now().Add(this.writeTimeout)); err != nil {
		return 0, this.errwrap(err)
	}
	c, err := this.Conn.WriteMessages(messages...)

	if err != nil {
		return 0, this.errwrap(err)
	}
	return c, nil
}

//将一条消息写入到kafka中
func (this *Kafka) WriteMessage(msg []byte) (int, error) {
	return this.Conn.WriteMessages(kafka.Message{Value: msg, Time: time.Now()})
}

//获取topic对象的所有分区信息, 拿到分区的总数量和下标位置后,可以再次实现化特别分区的Kafka对象进行操作
func (this *Kafka) LookupPartitions() ([]kafka.Partition, error) {
	return kafka.LookupPartitions(context.Background(), this.network, this.address, this.topic)
}

//添加Kafka报错标识
func (this *Kafka) errwrap(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("[Kafka]%s", err.Error())
}
