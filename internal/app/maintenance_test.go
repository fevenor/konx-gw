package app

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

func TestRetentionSurvivesFrequentClientSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st, err := events.Open(filepath.Join(t.TempDir(), "maintenance.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		cfg := config.Defaults()
		cfg.Events.MaxRows = 1
		for i := 0; i < 3; i++ {
			ev := events.Event{DeviceID: "10000001", Type: events.TypeDoorbellPress, TSRecv: time.Now().Unix()}
			if err := st.Insert(&ev); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		a := &App{Cfg: cfg, Store: st}
		go a.maintain(ctx)
		synctest.Wait()
		time.Sleep(12 * time.Hour)
		synctest.Wait()
		if n, _ := st.CountEvents(); n != 3 {
			t.Fatalf("清理提前发生：%d", n)
		}
		time.Sleep(12*time.Hour + time.Second)
		synctest.Wait()
		if n, _ := st.CountEvents(); n != 1 {
			t.Fatalf("24 小时后未按行数上限清理：%d", n)
		}
		cancel()
		synctest.Wait()
	})
}
