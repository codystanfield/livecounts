package pageviewExpirer

import (
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

type PageviewExpirer struct {
	redisClient          *redis.Client
	pages                map[string]bool // Poor man's set of pages
	pageTtlSeconds       int64
	expirationFrequency  time.Duration
	pageRefreshFrequency time.Duration
	quitChan             chan struct{}
}

func New(redisClient *redis.Client, pageTtlSeconds int64, expirationFrequency time.Duration, pageRefreshFrequency time.Duration) *PageviewExpirer {
	return &PageviewExpirer{
		redisClient:          redisClient,
		pages:                make(map[string]bool),
		pageTtlSeconds:       pageTtlSeconds,
		expirationFrequency:  expirationFrequency,
		pageRefreshFrequency: pageRefreshFrequency,
	}
}

func (pe *PageviewExpirer) Start() {
	go pe.start()
}

func (pe *PageviewExpirer) Stop() {
	close(pe.quitChan)
}

func (pe *PageviewExpirer) RegisterPage(pageKey string) {
	pe.pages[pageKey] = true
}

func (pe *PageviewExpirer) start() {
	pe.quitChan = make(chan struct{})

	// Get the initial page list
	pe.refreshPages()

	// Create tickers to launch goroutines
	expireTicker := time.NewTicker(pe.expirationFrequency)
	refreshTicker := time.NewTicker(pe.pageRefreshFrequency)

	for {
		select {
		case <-expireTicker.C:
			go pe.expirePageviews()
		case <-refreshTicker.C:
			go pe.refreshPages()
		case <-pe.quitChan:
			expireTicker.Stop()
			refreshTicker.Stop()
			return
		}
	}
}

func (pe *PageviewExpirer) expirePageviews() {
	lookback := time.Now().Unix() - pe.pageTtlSeconds

	for page := range pe.pages {
		maxScore := strconv.FormatInt(lookback, 10)
		_, err := pe.redisClient.ZRemRangeByScore(page, "0", maxScore).Result()
		if err != nil {
			fmt.Printf("Error removing items from %s: %s\n", page, err)
		}
	}
}

func (pe *PageviewExpirer) refreshPages() {
	// Update Pages from redis to get rid of old ones so we don't loop over them
	keys, err := pe.redisClient.Keys("visitorcount:*").Result()
	if err != nil {
		fmt.Printf("Error refreshing pages: %s", err)
		return
	}

	pe.pages = make(map[string]bool)
	for _, key := range keys {
		pe.pages[key] = true
	}
}
