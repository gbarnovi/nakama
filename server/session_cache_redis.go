package server

import (
	"context"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
)

type SessionStorageRedis struct {
	redis *redis.Client

	ctx         context.Context
	ctxCancelFn context.CancelFunc
}

func NewSessionStorageRedis(tokenExpirySec int64, rdb *redis.Client) *SessionStorageRedis {
	ctx, ctxCancelFn := context.WithCancel(context.Background())

	s := &SessionStorageRedis{
		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
		redis:       rdb,
	}
	//
	//go func() {
	//	ticker := time.NewTicker(2 * time.Duration(tokenExpirySec) * time.Second)
	//	for {
	//		select {
	//		case <-s.ctx.Done():
	//			ticker.Stop()
	//			return
	//		case t := <-ticker.C:
	//			tMs := t.UTC().Unix()
	//			for userID, cache := range s.cache {
	//				for token, exp := range cache.sessionTokens {
	//					if exp <= tMs {
	//						delete(cache.sessionTokens, token)
	//					}
	//				}
	//				for token, exp := range cache.refreshTokens {
	//					if exp <= tMs {
	//						delete(cache.refreshTokens, token)
	//					}
	//				}
	//				if len(cache.sessionTokens) == 0 && len(cache.refreshTokens) == 0 {
	//					delete(s.cache, userID)
	//				}
	//			}
	//		}
	//	}
	//}()

	return s
}

func (n *SessionStorageRedis) Stop() {
	n.ctxCancelFn()
}

func (n *SessionStorageRedis) IsValidSession(userID uuid.UUID, exp int64, token string) bool {
	res, err := n.redis.HGet(context.Background(), userID.String(), fmt.Sprintf("session_token_%v", token)).Result()
	if err != nil {
		return false
	}

	return len(res) != 0
}

func (n *SessionStorageRedis) IsValidRefresh(userID uuid.UUID, exp int64, token string) bool {
	res, err := n.redis.HGet(context.Background(), userID.String(), fmt.Sprintf("refresh_token_%v", token)).Result()
	if err != nil {
		return false
	}

	return len(res) != 0
}

func (n *SessionStorageRedis) Add(userID uuid.UUID, sessionExp int64, sessionToken string, refreshExp int64, refreshToken string) {
	n.redis.HIncrBy(context.Background(), userID.String(), fmt.Sprintf("session_token_%v", sessionToken), 1)
	n.redis.HIncrBy(context.Background(), userID.String(), fmt.Sprintf("refresh_token_%v", refreshToken), 1)
}

func (n *SessionStorageRedis) Remove(userID uuid.UUID, sessionExp int64, sessionToken string, refreshExp int64, refreshToken string) {
	if sessionToken != "" {
		n.redis.HDel(context.Background(), userID.String(), fmt.Sprintf("session_token_%v", sessionToken))
	}
	if refreshToken != "" {
		n.redis.HDel(context.Background(), userID.String(), fmt.Sprintf("session_token_%v", refreshToken))
	}
}

func (n *SessionStorageRedis) RemoveAll(userID uuid.UUID) {
	res, err := n.redis.HGetAll(context.Background(), userID.String()).Result()
	if err != nil {
		return
	}
	for s := range res {
		n.redis.HDel(context.Background(), userID.String(), s)
	}
}

func (n *SessionStorageRedis) Ban(userIDs []uuid.UUID) {
	//TODO implement me
	panic("implement me")
}

func (n *SessionStorageRedis) Unban(userIDs []uuid.UUID) {
	//TODO implement me
	panic("implement me")
}
