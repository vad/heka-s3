package s3

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	. "github.com/mozilla-services/heka/pipeline"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

const INTERVAL_PERIOD time.Duration = 24 * time.Hour
const HOUR_TO_TICK int = 00
const MINUTE_TO_TICK int = 00
const SECOND_TO_TICK int = 00

type S3OutputConfig struct {
	SecretKey        string `toml:"secret_key"`
	AccessKey        string `toml:"access_key"`
	Region           string `toml:"region"`
	Bucket           string `toml:"bucket"`
	Prefix           string `toml:"prefix"`
	TickerInterval   uint   `toml:"ticker_interval"`
	Compression      bool   `toml:"compression"`
	BufferPath       string `toml:"buffer_path"`
	BufferChunkLimit int    `toml:"buffer_chunk_limit"`
}

type S3Output struct {
	config         *S3OutputConfig
	client         *s3.S3
	bucket         *s3.Bucket
	bufferFilePath string
}

func midnightTickerUpdate() *time.Ticker {
	nextTick := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), HOUR_TO_TICK, MINUTE_TO_TICK, SECOND_TO_TICK, 0, time.Local)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(INTERVAL_PERIOD)
	}
	diff := nextTick.Sub(time.Now())
	return time.NewTicker(diff)
}

func (so *S3Output) ConfigStruct() interface{} {
	return &S3OutputConfig{Compression: true, BufferChunkLimit: 1000000}
}

func (so *S3Output) Init(config interface{}) (err error) {
	so.config = config.(*S3OutputConfig)
	auth, err := aws.GetAuth(so.config.AccessKey, so.config.SecretKey, "", time.Now())
	if err != nil {
		return
	}
	region, ok := aws.Regions[so.config.Region]
	if !ok {
		err = errors.New("Region of that name not found.")
		return
	}
	so.client = s3.New(auth, region)
	so.bucket = so.client.Bucket(so.config.Bucket)

	prefixList := strings.Split(so.config.Prefix, "/")
	bufferFileName := so.config.Bucket + strings.Join(prefixList, "_")
	so.bufferFilePath = so.config.BufferPath + "/" + bufferFileName
	return
}

func (so *S3Output) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()
	tickerChan := or.Ticker()
	buffer := bytes.NewBuffer(nil)
	midnightTicker := midnightTickerUpdate()

	var (
		pack     *PipelinePack
		outBytes []byte
		ok       = true
	)

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			var err error

			if outBytes, err = or.Encode(pack); err != nil {
				or.LogError(fmt.Errorf("Error encoding message: %s", err))
			} else if outBytes != nil {
				err = so.WriteToBuffer(buffer, outBytes, or)
			}
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to write to buffer: %s", err))
				err = nil
				continue
			}
			pack.Recycle(nil)
		case <-tickerChan:
			or.LogMessage(fmt.Sprintf("Ticker fired, uploading payload."))
			err := so.Upload(buffer, or, false)
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to upload payload: %s", err))
				err = nil
				continue
			}
			or.LogMessage(fmt.Sprintf("Payload uploaded successfully."))
			buffer.Reset()
		case <-midnightTicker.C:
			midnightTicker = midnightTickerUpdate()
			or.LogMessage(fmt.Sprintf("Midnight ticker fired, uploading payload."))
			err := so.Upload(buffer, or, true)
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to upload payload: %s", err))
				err = nil
				continue
			}
			or.LogMessage(fmt.Sprintf("Payload uploaded successfully."))
			buffer.Reset()
		}
	}

	or.LogMessage(fmt.Sprintf("Shutting down S3 output runner."))
	return
}

func (so *S3Output) WriteToBuffer(buffer *bytes.Buffer, outBytes []byte, or OutputRunner) (err error) {
	_, err = buffer.Write(outBytes)
	if err != nil {
		return
	}
	if buffer.Len() > so.config.BufferChunkLimit {
		err = so.SaveToDisk(buffer, or)
	}
	return
}

func (so *S3Output) SaveToDisk(buffer *bytes.Buffer, or OutputRunner) (err error) {
	_, err = os.Stat(so.config.BufferPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(so.config.BufferPath, 0666)
		if err != nil {
			return
		}
	}

	err = os.Chdir(so.config.BufferPath)
	if err != nil {
		return
	}

	_, err = os.Stat(so.bufferFilePath)
	if os.IsNotExist(err) {
		or.LogMessage("Creating buffer file: " + so.bufferFilePath)
		w, err := os.Create(so.bufferFilePath)
		w.Close()
		if err != nil {
			return err
		}
	}

	f, err := os.OpenFile(so.bufferFilePath, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	_, err = f.Write(buffer.Bytes())
	if err != nil {
		return
	}

	buffer.Reset()

	return
}

func (so *S3Output) ReadFromDisk(or OutputRunner) (buffer *bytes.Buffer, err error) {
	if so.config.Compression {
		or.LogMessage("Compressing buffer file...")
		cmd := exec.Command("gzip", so.bufferFilePath)
		err = cmd.Run()
		if err != nil {
			return nil, err
		}
		// rename to original filename without .gz extension
		cmd = exec.Command("mv", so.bufferFilePath+".gz", so.bufferFilePath)
		err = cmd.Run()
		if err != nil {
			return nil, err
		}
	}

	or.LogMessage("Uploading, reading from buffer file.")
	fi, err := os.Open(so.bufferFilePath)
	if err != nil {
		return
	}
	defer fi.Close()

	r := bufio.NewReader(fi)
	buffer = bytes.NewBuffer(nil)

	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			break
		}
		if n == 0 {
			break
		}
		_, err = buffer.Write(buf[:n])
		if err != nil {
			break
		}
	}

	return buffer, err
}

func (so *S3Output) Upload(buffer *bytes.Buffer, or OutputRunner, isMidnight bool) (err error) {
	_, err = os.Stat(so.bufferFilePath)
	if buffer.Len() == 0 && os.IsNotExist(err) {
		err = errors.New("Nothing to upload.")
		return
	}

	err = so.SaveToDisk(buffer, or)
	if err != nil {
		return
	}

	buffer, err = so.ReadFromDisk(or)
	if err != nil {
		return
	}

	var (
		currentTime = time.Now().Local().Format("20060102150405")
		currentDate = ""
		ext         = ""
		contentType = "text/plain"
	)

	if isMidnight {
		currentDate = time.Now().Local().AddDate(0, 0, -1).Format("2006-01-02 15:00:00 +0800")[0:10]
	} else {
		currentDate = time.Now().Local().Format("2006-01-02 15:00:00 +0800")[0:10]
	}

	if so.config.Compression {
		ext = ".gz"
		contentType = "multipart/x-gzip"
	}

	path := so.config.Prefix + "/" + currentDate + "/" + currentTime + ext
	err = so.bucket.Put(path, buffer.Bytes(), contentType, "public-read", s3.Options{})

	or.LogMessage("Upload finished, removing buffer file on disk.")
	if err == nil {
		err = os.Remove(so.bufferFilePath)
	}

	return
}

func init() {
	RegisterPlugin("S3Output", func() interface{} {
		return new(S3Output)
	})
}
