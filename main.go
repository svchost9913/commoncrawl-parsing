package main

import (
	"bufio"
	"commoncrawl-parsing/logs"
	"context"
	"fmt"
	"github.com/elliotwutingfeng/go-fasttld"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

var redisClient *redis.Client

type result struct {
	domain string
	err    error
}

func saveDomainsToRedis(ctx context.Context, redisClient *redis.Client, filePath string, domainKey string) error {
	file, err := os.Open(filePath)
	if err != nil {
		logs.Error("无法打开文件", zap.Error(err))
		return fmt.Errorf("无法打开文件：%w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logs.Error("关闭文件时出错", zap.Error(err))
		}
	}(file)

	// 逐行读取文件，减少内存占用
	scanner := bufio.NewScanner(bufio.NewReader(file))

	// 使用 sync.Map 存储结果
	resultMap := new(sync.Map)

	// 并发处理
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			extractor, _ := fasttld.New(fasttld.SuffixListParams{})
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "id") {
					re := regexp.MustCompile(`"url": "([^"]+)"`)
					match := re.FindStringSubmatch(line)
					if len(match) > 1 {
						urlStr := match[1]
						parse, err := url.Parse(urlStr)
						res, _ := extractor.Extract(fasttld.URLParams{URL: urlStr, IgnoreSubDomains: true})
						if err != nil {
							resultMap.Store(urlStr, result{err: fmt.Errorf("解析 URL %s 时出错：%v", urlStr, err)})
							continue
						}
						if parse.RawQuery != "" {
							if err := redisClient.SAdd(ctx, "WaitingSql", parse.String()).Err(); err != nil {
								resultMap.Store(urlStr, result{err: fmt.Errorf("保存 URL %s 到 Redis 时出错：%v", urlStr, err)})
								continue
							}
							logs.Info("已将 URL保存到 Redis", zap.String("url", urlStr))
						}
						domain := fmt.Sprintf("%s%s", res.Scheme, res.RegisteredDomain)
						if domain == "" {
							continue
						}
						resultMap.Store(domain, result{domain: domain})
					}
				}
			}
		}()
	}
	wg.Wait()

	// 使用批量操作
	var domains []string
	resultMap.Range(func(key, value interface{}) bool {
		switch res := value.(type) {
		case result:
			if err != nil {
				logs.Error("处理数据时出错", zap.Error(err))
			} else {
				domains = append(domains, res.domain)
			}
		default:
			log.Printf("类型断言失败: key=%v, value=%v", key, value)
		}
		return true
	})
	if len(domains) > 0 {
		if err := redisClient.SAdd(ctx, domainKey, domains).Err(); err != nil {
			logs.Error("保存主域名到 Redis 时出错", zap.Strings("domains", domains), zap.Error(err))
			return fmt.Errorf("保存主域名到 Redis 时出错: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {

		logs.Error("扫描文件时出错", zap.Error(err))
		return fmt.Errorf("扫描文件出错: %w", err)
	}
	return nil
}
func initRedis() {
	viper.SetConfigFile("config.ini")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.addr"),
		Password: viper.GetString("redis.password"),
		DB:       viper.GetInt("redis.db"),
	})
}
func main() {
	initRedis()
	// 使用 Context.Background() 作为参数
	err := saveDomainsToRedis(context.Background(), redisClient, "1.txt", viper.GetString("redis.domain_set"))
	if err != nil {
		logs.Error("处理数据时出错", zap.Error(err))
		return
	}
	logs.Info("处理完毕！")
}
