package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"gitlab.com/useproof/livecounts/pageviewExpirer"
	"os"
	"time"
)

func main() {
	redisEndpoint := os.Getenv("REDIS_ENDPOINT")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisEndpoint,
		Password: redisPassword,
		DB:       0,
	})

	expirer := pageviewExpirer.New(redisClient, 30, time.Second*5, time.Second*30)
	expirer.Start()

	r := gin.Default()

	r.GET("/live-pageview-count/:pageId", func(c *gin.Context) {
		pageId := c.Param("pageId")
		pageKey := createVisitorCountKey(pageId)

		cardinality, err := redisClient.ZCard(pageKey).Result()
		if err != nil {
			fmt.Printf("Error getting live page count for key %s: %s", pageKey, err)
			c.JSON(500, gin.H{
				"error": err,
			})
			return
		}

		c.JSON(200, gin.H{
			"livePageviews": cardinality,
		})
	})

	r.POST("/live-pageview-count/:pageId", func(c *gin.Context) {
		pageId := c.Param("pageId")
		pageKey := createVisitorCountKey(pageId)
		visitorId := c.Query("visitorId")

		// Use timestamp in seconds for score
		score := time.Now().Unix()
		redisClient.ZAdd(pageKey, redis.Z{Score: float64(score), Member: visitorId})
		expirer.RegisterPage(pageKey)
	})

	// TODO: don't want to expose start or stop in prod or staging
	// TODO: need to add real docstring comments
	r.POST("/start", func(c *gin.Context) {
		// TODO: don't want multiple running at once
		expirer.Start()
	})

	r.POST("/stop", func(c *gin.Context) {
		expirer.Stop()
	})

	// TODO: allow for different environments (dev, staging, prod)
	r.Run()
}

func createVisitorCountKey(pageId string) string {
	return "visitorcount:" + pageId
}
