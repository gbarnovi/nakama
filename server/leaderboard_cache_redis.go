// Copyright 2018 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/redis/go-redis/v9"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/heroiclabs/nakama/v3/internal/cronexpr"
	"github.com/jackc/pgtype"
	"go.uber.org/zap"
)

type LocalLeaderboardCacheRedis struct {
	redis  *redis.Client
	logger *zap.Logger
	db     *sql.DB
}

func (l LocalLeaderboardCacheRedis) RefreshAllLeaderboards(ctx context.Context) error {
	query := `
SELECT
id, authoritative, sort_order, operator, reset_schedule, metadata, create_time,
category, description, duration, end_time, join_required, max_size, max_num_score, title, start_time
FROM leaderboard`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		l.logger.Error("Error loading leaderboard cache from database", zap.Error(err))
		return err
	}

	tournamentList := make([]*Leaderboard, 0)
	leaderboardList := make([]*Leaderboard, 0)

	for rows.Next() {
		var id string
		var authoritative bool
		var sortOrder int
		var operator int
		var resetSchedule sql.NullString
		var metadata string
		var createTime pgtype.Timestamptz
		var category int
		var description string
		var duration int
		var endTime pgtype.Timestamptz
		var joinRequired bool
		var maxSize int
		var maxNumScore int
		var title string
		var startTime pgtype.Timestamptz

		err = rows.Scan(&id, &authoritative, &sortOrder, &operator, &resetSchedule, &metadata, &createTime,
			&category, &description, &duration, &endTime, &joinRequired, &maxSize, &maxNumScore, &title, &startTime)
		if err != nil {
			_ = rows.Close()
			l.logger.Error("Error parsing leaderboard cache from database", zap.Error(err))
			return err
		}

		leaderboard := &Leaderboard{
			Id:            id,
			Authoritative: authoritative,
			SortOrder:     sortOrder,
			Operator:      operator,

			Metadata:     metadata,
			CreateTime:   createTime.Time.Unix(),
			Category:     category,
			Description:  description,
			Duration:     duration,
			EndTime:      0,
			JoinRequired: joinRequired,
			MaxSize:      maxSize,
			MaxNumScore:  maxNumScore,
			Title:        title,
			StartTime:    startTime.Time.Unix(),
		}
		if resetSchedule.Valid {
			expr, err := cronexpr.Parse(resetSchedule.String)
			if err != nil {
				_ = rows.Close()
				l.logger.Error("Error parsing leaderboard reset schedule from database", zap.Error(err))
				return err
			}
			leaderboard.ResetScheduleStr = resetSchedule.String
			leaderboard.ResetSchedule = expr
		}
		if endTime.Status == pgtype.Present {
			leaderboard.EndTime = endTime.Time.Unix()
		}

		l.AddToLeaderboard(id, leaderboard)

		if leaderboard.IsTournament() {
			tournamentList = append(tournamentList, leaderboard)
		} else {
			leaderboardList = append(leaderboardList, leaderboard)
		}
	}
	_ = rows.Close()

	sort.Sort(OrderedTournaments(tournamentList))

	encodedtournamentList, err := json.Marshal(tournamentList)
	if err != nil {
		l.logger.Error(err.Error())
	}

	encodedleaderboardList, err := json.Marshal(leaderboardList)
	if err != nil {
		l.logger.Error(err.Error())
	}

	l.redis.Set(ctx, "tournamentList", encodedtournamentList, 0)
	l.redis.Set(ctx, "leaderboardList", encodedleaderboardList, 0)

	return nil
}

func NewLocalLeaderboardCacheRedis(logger, startupLogger *zap.Logger, db *sql.DB, redis *redis.Client) LeaderboardCache {
	l := &LocalLeaderboardCacheRedis{
		logger: logger,
		db:     db,
		redis:  redis,
	}

	err := l.RefreshAllLeaderboards(context.Background())
	if err != nil {
		startupLogger.Fatal("Error loading leaderboard cache from database", zap.Error(err))
	}

	return l
}

func (l LocalLeaderboardCacheRedis) Get(id string) *Leaderboard {
	var result Leaderboard
	res, err := l.redis.HGet(context.Background(), "leaderboards", id).Result()
	if err != nil {
		l.logger.Error("failed to get leaderboard", zap.String("id", id), zap.Error(err))
		return nil
	}

	l.logger.Info("HGet result", zap.String("res", res))

	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		l.logger.Error("failed to unmarshal JSON", zap.String("json", res), zap.Error(err))
		return nil
	}

	expr, err := cronexpr.Parse(result.ResetScheduleStr)
	if err != nil {
		l.logger.Error("error parsing leaderboard reset schedule from database", zap.Error(err))
	}
	result.ResetSchedule = expr

	return &result
}

func (l LocalLeaderboardCacheRedis) AddToLeaderboard(id string, leaderboard *Leaderboard) {
	encoded, _ := json.Marshal(leaderboard)
	err := l.redis.HSet(context.Background(), "leaderboards", id, encoded).Err()
	if err != nil {
		l.logger.Error("error AddToLeaderboard", zap.Error(err))
	}
}

func (l LocalLeaderboardCacheRedis) GetAllLeaderboards() []*Leaderboard {
	var result []*Leaderboard
	ctx := context.Background()

	// Get all entries from the "leaderboards" hash
	data, err := l.redis.HGetAll(ctx, "leaderboards").Result()
	if err != nil {
		l.logger.Error("error fetching leaderboard data from database", zap.Error(err))
		return []*Leaderboard{}
	}

	// Convert the map data to a slice of Leaderboard structs
	for _, jsonData := range data {
		var leaderboard Leaderboard
		err := json.Unmarshal([]byte(jsonData), &leaderboard)
		if err != nil {
			l.logger.Error("error unmarshaling leaderboard data", zap.String("data", jsonData), zap.Error(err))
			continue
		}

		result = append(result, &leaderboard)
	}

	return result
}

func (l LocalLeaderboardCacheRedis) Create(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata string) (*Leaderboard, error) {
	var expr *cronexpr.Expression

	var err error
	if resetSchedule != "" {
		expr, err = cronexpr.Parse(resetSchedule)
		if err != nil {
			l.logger.Error("Error parsing leaderboard reset schedule", zap.Error(err))
			return nil, err
		}
	}

	// Insert into database first.
	query := "INSERT INTO leaderboard (id, authoritative, sort_order, operator, metadata"
	if resetSchedule != "" {
		query += ", reset_schedule"
	}
	query += ") VALUES ($1, $2, $3, $4, $5"
	if resetSchedule != "" {
		query += ", $6"
	}
	query += ") RETURNING create_time"
	params := []interface{}{id, authoritative, sortOrder, operator, metadata}
	if resetSchedule != "" {
		params = append(params, resetSchedule)
	}
	var createTime pgtype.Timestamptz
	err = l.db.QueryRowContext(ctx, query, params...).Scan(&createTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == dbErrorUniqueViolation {
			// Concurrent attempt at creating the leaderboard, to keep idempotency query the existing leaderboard data.
			if err = l.db.QueryRowContext(ctx, "SELECT authoritative, sort_order, operator, COALESCE(reset_schedule, ''), metadata, create_time FROM leaderboard WHERE id = $1", id).Scan(&authoritative, &sortOrder, &operator, &resetSchedule, &metadata, &createTime); err != nil {
				l.logger.Error("Error retrieving leaderboard", zap.Error(err))
				return nil, err
			}
			if resetSchedule != "" {
				expr, err = cronexpr.Parse(resetSchedule)
				if err != nil {
					l.logger.Error("Error parsing leaderboard reset schedule", zap.Error(err))
					return nil, err
				}
			}
		} else {
			l.logger.Error("Error creating leaderboard", zap.Error(err))
			return nil, err
		}
	}
	l.logger.Info("after query")

	// Then add to cache.
	leaderboard := &Leaderboard{
		Id:               id,
		Authoritative:    authoritative,
		SortOrder:        sortOrder,
		Operator:         operator,
		ResetScheduleStr: resetSchedule,
		ResetSchedule:    expr,
		Metadata:         metadata,
		CreateTime:       createTime.Time.Unix(),
	}

	l.logger.Info("calling from create")
	l.AddToLeaderboard(id, leaderboard)
	l.AddToList(ctx, "leaderboardList", leaderboard)

	return leaderboard, nil
}

func (l LocalLeaderboardCacheRedis) Insert(id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata string, createTime int64) {
	var expr *cronexpr.Expression
	var err error
	if resetSchedule != "" {
		expr, err = cronexpr.Parse(resetSchedule)
		if err != nil {
			// Not expected, this insert is as a result of a previous create that has succeeded.
			l.logger.Error("Error parsing leaderboard reset schedule for insert", zap.Error(err))
			return
		}
	}

	leaderboard := &Leaderboard{
		Id:               id,
		Authoritative:    authoritative,
		SortOrder:        sortOrder,
		Operator:         operator,
		ResetScheduleStr: resetSchedule,
		ResetSchedule:    expr,
		Metadata:         metadata,
		CreateTime:       createTime,
	}

	l.logger.Info("calling from insert")
	l.AddToLeaderboard(id, leaderboard)
	l.AddToList(context.Background(), "leaderboardList", leaderboard)
}

func (l LocalLeaderboardCacheRedis) AddToList(ctx context.Context, key string, leaderboard *Leaderboard) {
	var leaderboardList []*Leaderboard

	data, err := l.redis.HGetAll(ctx, "leaderboards").Result()
	if err != nil {
		l.logger.Error("error fetching leaderboard data from database", zap.Error(err))
		return
	}

	// Convert the map data to a slice of Leaderboard structs
	for _, jsonData := range data {
		var ld Leaderboard
		err := json.Unmarshal([]byte(jsonData), &ld)
		if err != nil {
			l.logger.Error("error unmarshalling leaderboard data", zap.String("data", jsonData), zap.Error(err))
			continue
		}
		leaderboardList = append(leaderboardList, &ld)
	}

	sort.Sort(OrderedTournaments(leaderboardList))

	l.logger.Info("leaderboard length: ", zap.Int("len", len(leaderboardList)))

	encoded, err := json.Marshal(leaderboardList)
	if err != nil {
		l.logger.Error(err.Error())
	}

	res := l.redis.Set(ctx, key, encoded, 0)
	if res.Err() != nil {
		l.logger.Info(string(encoded))
		l.logger.Error(res.Err().Error())
	}
}

func (l LocalLeaderboardCacheRedis) List(categoryStart, categoryEnd, limit int, cursor *LeaderboardListCursor) ([]*Leaderboard, *LeaderboardListCursor, error) {
	list := make([]*Leaderboard, 0, limit)
	var newCursor *TournamentListCursor
	skip := cursor != nil

	res, err := l.redis.HGetAll(context.Background(), "leaderboardList").Result()
	if err != nil {
		return nil, nil, err
	}

	for _, s := range res {
		var leaderboard *Leaderboard
		if err := json.Unmarshal([]byte(s), leaderboard); err != nil {
			return nil, nil, err
		}

		if skip {
			if leaderboard.Id == cursor.Id {
				skip = false
			}
			continue
		}

		if leaderboard.Category < categoryStart {
			// Skip tournaments with category before start boundary.
			continue
		}
		if leaderboard.Category > categoryEnd {
			// Skip tournaments with category after end boundary.
			continue
		}

		if ln := len(list); ln >= limit {
			newCursor = &LeaderboardListCursor{
				Id: list[ln-1].Id,
			}
			break
		}

		list = append(list, leaderboard)

	}

	return list, newCursor, nil
}

func (l LocalLeaderboardCacheRedis) CreateTournament(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata, title, description string, category, startTime, endTime, duration, maxSize, maxNumScore int, joinRequired bool) (*Leaderboard, error) {
	resetCron, err := checkTournamentConfig(resetSchedule, startTime, endTime, duration, maxSize, maxNumScore)
	if err != nil {
		l.logger.Error("Error while creating tournament", zap.Error(err))
		return nil, err
	}

	leaderboard := l.Get(id)

	if leaderboard != nil {
		if leaderboard.IsTournament() {
			// Creation is an idempotent operation.
			return leaderboard, nil
		}
		l.logger.Error("Cannot create tournament as leaderboard is already in use.", zap.String("leaderboard_id", id))
		return nil, fmt.Errorf("cannot create tournament as leaderboard is already in use")
	}

	params := []interface{}{id, authoritative, sortOrder, operator, duration}
	columns := "id, authoritative, sort_order, operator, duration"
	values := "$1, $2, $3, $4, $5"

	if resetSchedule != "" {
		params = append(params, resetSchedule)
		columns += ", reset_schedule"
		values += ", $" + strconv.Itoa(len(params))
	}

	if metadata != "" {
		params = append(params, metadata)
		columns += ", metadata"
		values += ", $" + strconv.Itoa(len(params))
	}

	if category >= 0 {
		params = append(params, category)
		columns += ", category"
		values += ", $" + strconv.Itoa(len(params))
	}

	if description != "" {
		params = append(params, description)
		columns += ", description"
		values += ", $" + strconv.Itoa(len(params))
	}

	if endTime > 0 {
		params = append(params, time.Unix(int64(endTime), 0).UTC())
		columns += ", end_time"
		values += ", $" + strconv.Itoa(len(params))
	}

	if joinRequired {
		params = append(params, joinRequired)
		columns += ", join_required"
		values += ", $" + strconv.Itoa(len(params))
	}

	if maxSize == 0 {
		maxSize = math.MaxInt32
	}
	if maxSize > 0 {
		params = append(params, maxSize)
		columns += ", max_size"
		values += ", $" + strconv.Itoa(len(params))
	}

	if maxNumScore > 0 {
		params = append(params, maxNumScore)
		columns += ", max_num_score"
		values += ", $" + strconv.Itoa(len(params))
	}

	if title != "" {
		params = append(params, title)
		columns += ", title"
		values += ", $" + strconv.Itoa(len(params))
	}

	if startTime > 0 {
		params = append(params, time.Unix(int64(startTime), 0).UTC())
		columns += ", start_time"
		values += ", $" + strconv.Itoa(len(params))
	}

	query := "INSERT INTO leaderboard (" + columns + ") VALUES (" + values + ") RETURNING metadata, max_size, max_num_score, create_time, start_time, end_time"

	l.logger.Debug("Create tournament query", zap.String("query", query))

	var dbMetadata string
	var dbMaxSize int
	var dbMaxNumScore int
	var createTime pgtype.Timestamptz
	var dbStartTime pgtype.Timestamptz
	var dbEndTime pgtype.Timestamptz
	err = l.db.QueryRowContext(ctx, query, params...).Scan(&dbMetadata, &dbMaxSize, &dbMaxNumScore, &createTime, &dbStartTime, &dbEndTime)
	if err != nil {
		l.logger.Error("Error creating tournament", zap.Error(err))
		return nil, err
	}

	leaderboard = &Leaderboard{
		Id:               id,
		Authoritative:    authoritative,
		SortOrder:        sortOrder,
		Operator:         operator,
		ResetScheduleStr: resetSchedule,
		ResetSchedule:    resetCron,
		Metadata:         dbMetadata,
		CreateTime:       createTime.Time.Unix(),
		Category:         category,
		Description:      description,
		Duration:         duration,
		EndTime:          0,
		JoinRequired:     joinRequired,
		MaxSize:          dbMaxSize,
		MaxNumScore:      dbMaxNumScore,
		Title:            title,
		StartTime:        dbStartTime.Time.Unix(),
	}
	if dbEndTime.Status == pgtype.Present {
		leaderboard.EndTime = dbEndTime.Time.Unix()
	}

	l.AddToLeaderboard(id, leaderboard)

	l.AddToList(context.Background(), "tournamentList", leaderboard)

	return leaderboard, nil
}

func (l LocalLeaderboardCacheRedis) InsertTournament(id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata, title, description string, category, duration, maxSize, maxNumScore int, joinRequired bool, createTime, startTime, endTime int64) {
	var expr *cronexpr.Expression
	var err error
	if resetSchedule != "" {
		expr, err = cronexpr.Parse(resetSchedule)
		if err != nil {
			// Not expected, this insert is as a result of a previous create that has succeeded.
			l.logger.Error("Error parsing tournament reset schedule for insert", zap.Error(err))
			return
		}
	}

	leaderboard := &Leaderboard{
		Id:               id,
		Authoritative:    authoritative,
		SortOrder:        sortOrder,
		Operator:         operator,
		ResetScheduleStr: resetSchedule,
		ResetSchedule:    expr,
		Metadata:         metadata,
		CreateTime:       createTime,
		Category:         category,
		Description:      description,
		Duration:         duration,
		JoinRequired:     joinRequired,
		MaxSize:          maxSize,
		MaxNumScore:      maxNumScore,
		Title:            title,
		StartTime:        startTime,
		EndTime:          endTime,
	}

	l.AddToLeaderboard(id, leaderboard)
	l.AddToList(context.Background(), "tournamentList", leaderboard)
}

func (l LocalLeaderboardCacheRedis) ListTournaments(now int64, categoryStart, categoryEnd int, startTime, endTime int64, limit int, cursor *TournamentListCursor) ([]*Leaderboard, *TournamentListCursor, error) {
	list := make([]*Leaderboard, 0, limit)
	var newCursor *TournamentListCursor
	skip := cursor != nil

	res, err := l.redis.Get(context.Background(), "tournamentList").Result()
	if err != nil {
		l.logger.Error("Error getting tournament list", zap.Error(err))
		return nil, nil, err
	}

	var leaderboards []*Leaderboard
	if err := json.Unmarshal([]byte(res), &leaderboards); err != nil {
		l.logger.Error("Error parsing tournament list", zap.Error(err))
		return nil, nil, err
	}

	for _, leaderboard := range leaderboards {
		if skip {
			if leaderboard.Id == cursor.Id {
				skip = false
			}
			continue
		}

		if leaderboard.Category < categoryStart {
			// Skip tournaments with category before start boundary.
			continue
		}
		if leaderboard.Category > categoryEnd {
			// Skip tournaments with category after end boundary.
			continue
		}
		if leaderboard.StartTime < startTime {
			// Skip tournaments with start time before filter.
			continue
		}
		if (endTime == 0 && leaderboard.EndTime != 0) || (endTime == -1 && (leaderboard.EndTime != 0 && leaderboard.EndTime < now)) || (endTime > 0 && (leaderboard.EndTime == 0 || leaderboard.EndTime > endTime)) {
			// if (endTime == 0 && leaderboard.EndTime != 0) || (endTime == -1 && endTime < now) ||leaderboard.EndTime > endTime || leaderboard.EndTime == 0) || leaderboard.EndTime > endTime {
			// SKIP tournaments where:
			// - If end time filter is == 0, tournament end time is non-0.
			// - If end time filter is default (show only ongoing/future tournaments) and tournament has ended.
			// - If end time filter is set and tournament end time is below it.
			continue
		}

		if ln := len(list); ln >= limit {
			newCursor = &TournamentListCursor{
				Id: list[ln-1].Id,
			}
			break
		}

		list = append(list, leaderboard)
	}

	return list, newCursor, nil
}

func (l LocalLeaderboardCacheRedis) Delete(ctx context.Context, rankCache LeaderboardRankCache, scheduler LeaderboardScheduler, id string) error {
	// Delete from database first.
	query := "DELETE FROM leaderboard WHERE id = $1"
	_, err := l.db.ExecContext(ctx, query, id)
	if err != nil {
		l.logger.Error("Error deleting leaderboard", zap.Error(err))
		return err
	}

	l.redis.HDel(context.Background(), "leaderboards", id)

	l.deleteFromList("tournamentList", id)
	l.deleteFromList("leaderboardList", id)

	scheduler.Update()

	//if expiryUnix > now.Unix() {
	//	// Clear any cached ranks that have not yet expired.
	//	rankCache.DeleteLeaderboard(id, expiryUnix)
	//}

	return nil
}

func (l LocalLeaderboardCacheRedis) deleteFromList(key string, id string) ([]Leaderboard, error) {
	result, err := l.redis.Get(context.Background(), key).Result()
	if err != nil {
		l.logger.Error("%v", zap.Error(err))
	}
	var res []Leaderboard
	if err := json.Unmarshal([]byte(result), &res); err != nil {
		l.logger.Error("%v", zap.Error(err))
		return []Leaderboard{}, err
	}

	if err != nil {
		l.logger.Error("error: %v", zap.Error(err))
	}

	for i, leaderboard := range res {
		if leaderboard.Id == id {
			res = append(res[:i], res[i+1:]...)
			break
		}
	}

	encoded, err := json.Marshal(res)
	if err := json.Unmarshal([]byte(result), &res); err != nil {
		l.logger.Error("%v", zap.Error(err))
		return []Leaderboard{}, err
	}

	l.redis.Set(context.Background(), key, encoded, 0)

	return []Leaderboard{}, nil
}

func (l LocalLeaderboardCacheRedis) Remove(id string) {
	l.redis.HDel(context.Background(), "leaderboards", id)
	l.deleteFromList("tournamentList", id)
	l.deleteFromList("leaderboardList", id)
}
