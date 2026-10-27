// bindcheck: one-shot live query of the account's bound devices.
// 默认走现行云端端点 Relationship/LoadDeviceInfo.ashx；KONX_DEVICE_LIST
// 可固定 auto|loaddeviceinfo|searchbinddev（默认 auto：失败/空时降级旧端点
// SearchBindDev.ashx）。
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/fevenor/konx-gw/internal/cloud"
)

func main() {
	user := os.Getenv("KONX_CLOUD_USER")
	pass := os.Getenv("KONX_CLOUD_PASS")
	cc := cloud.NewClient("api1.cloudlinks.cn")
	if mode := cloud.DeviceListMode(os.Getenv("KONX_DEVICE_LIST")); mode != "" {
		cc.ListMode = mode
	}
	cs, err := cc.Login(context.Background(), cloud.Credentials{User: user, Pass: pass})
	if err != nil {
		fmt.Println("LOGIN FAIL:", err)
		os.Exit(1)
	}
	fmt.Printf("LOGIN OK uid=%d sid1=%d\n", cs.UID, cs.S1)
	devs, err := cc.ListDevices(context.Background(), cs)
	if err != nil {
		fmt.Println("ListDevices:", err)
		os.Exit(1)
	}
	if len(devs) == 0 {
		fmt.Println("BOUND DEVICES: none")
		return
	}
	for _, d := range devs {
		fmt.Printf("BOUND: id=%s name=%s cate=%d doorbell=%v secret_key=%s online=%v\n",
			d.DevID, d.Name, d.DeviceCate, d.IsDoorbell(), d.SecretKey, d.Online)
		if d.Raw != nil {
			fmt.Printf("       raw=%v\n", d.Raw)
		}
	}
}
