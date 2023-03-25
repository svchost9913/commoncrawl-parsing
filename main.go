package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"github.com/svchost9913/commoncrawl-parsing/logs"
	"go.uber.org/zap"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
)

var redisClient *redis.Client
var group sync.WaitGroup
var (
	_ sync.Mutex
)

func initRedis() {
	viper.SetConfigFile("config.ini")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	poolSize := viper.GetInt("redis.pool_size")
	redisClient = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.addr"),
		Password: viper.GetString("redis.password"),
		DB:       viper.GetInt("redis.db"),
		PoolSize: poolSize,
	})
}

func saveDomainsToRedis(ctx context.Context, redisClient *redis.Client, filePath string, key string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件：%w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	domainSet := map[string]bool{}
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id") {
			re := regexp.MustCompile(`"url": "([^"]+)"`)
			match := re.FindStringSubmatch(line)
			if len(match) > 1 {
				urls := match[1]
				// 如果这个域名已经被处理过，则跳过
				if domainSet[urls] {
					continue
				}
				domainSet[urls] = true
				tx := redisClient.TxPipeline()
				err = tx.SAdd(ctx, key, urls).Err()
				if err != nil {
					fmt.Printf("保存主域名 %s 到 Redis 时出错：%v\n", urls, err)
					err := tx.Discard()
					if err != nil {
						log.Println(err)
					}
					continue
				}
				parsedURL, _ := url.Parse(urls)
				// 如果 URL 包含查询参数，则保存到 Redis
				if parsedURL.RawQuery != "" {
					err = tx.SAdd(ctx, "WaitingSql", parsedURL.String()).Err()
					if err != nil {
						fmt.Printf("保存 URL %s 到 Redis 时出错：%v\n", urls, err)
						err := tx.Discard()
						if err != nil {
							log.Println(err)
						}
						continue
					}
					fmt.Printf("已将 URL %s 保存到 Redis.\n", urls)
				}
				_, err = tx.Exec(ctx)
				if err != nil {
					log.Printf("tx.Exec() error: %v\n", err)
					err := tx.Discard()
					if err != nil {
						log.Println(err)
					}
					continue
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("扫描文件出错: %w", err)
	}

	return nil
}
func main() {
	initRedis()
	// 使用 Context.Background() 作为参数
	err := saveDomainsToRedis(context.Background(), redisClient, "cdx-00248", viper.GetString("redis.domain_set"))
	if err != nil {
		logs.Error("处理数据时出错 \n", zap.Error(err))
		return
	}
	logs.Info("处理完毕！")
}
