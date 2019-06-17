package prometheus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/prometheus/prometheus/storage"
)

// Querier returns a new Querier on the storage.
func (h *Handler) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &Querier{
		config: h.config,
		ctx:    ctx,
		mint:   mint,
		maxt:   maxt,
	}, nil
}

// Querier provides reading access to time series data.
type Querier struct {
	config *config.Config
	mint   int64
	maxt   int64
	ctx    context.Context
}

// Close releases the resources of the Querier.
func (q *Querier) Close() error {
	return nil
}

// LabelValues returns all potential values for a label name.
func (q *Querier) LabelValues(label string) ([]string, error) {
	where := finder.NewWhere()
	where.Andf("Tag1 LIKE %s", finder.Q(finder.LikeEscape(label)+"=%"))

	fromDate := time.Now().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
	where.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[2] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		where.SQL(),
	)

	body, err := clickhouse.Query(q.ctx, q.config.ClickHouse.Url, sql, q.config.ClickHouse.TaggedTable,
		clickhouse.Options{Timeout: q.config.ClickHouse.IndexTimeout.Value(), ConnectTimeout: q.config.ClickHouse.ConnectTimeout.Value()})
	if err != nil {
		return nil, err
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	return rows, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *Querier) LabelNames() ([]string, error) {
	where := finder.NewWhere()
	fromDate := time.Now().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
	where.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[1] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		where.SQL(),
	)

	body, err := clickhouse.Query(q.ctx, q.config.ClickHouse.Url, sql, q.config.ClickHouse.TaggedTable,
		clickhouse.Options{Timeout: q.config.ClickHouse.IndexTimeout.Value(), ConnectTimeout: q.config.ClickHouse.ConnectTimeout.Value()})
	if err != nil {
		return nil, err
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	return rows, nil
}