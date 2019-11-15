package statemanager

import "sync"

type manager struct {
	state string

	// InfluxDB V2
	v2Host    string
	orgId     string
	bucketId  string
	authToken string
}

var singleton *manager
var once sync.Once

func GetManager() *manager {
	once.Do(func() {
		singleton = &manager{state: "off"}
	})
	return singleton
}
func (sm *manager) GetState() string {
	return sm.state
}
func (sm *manager) SetState(s string) {
	sm.state = s
}
func (sm *manager) Getv2Host() string {
	return sm.v2Host
}
func (sm *manager) Setv2Host(s string) {
	sm.v2Host = s
}
func (sm *manager) GetOrgId() string {
	return sm.orgId
}
func (sm *manager) SetOrgId(s string) {
	sm.orgId = s
}
func (sm *manager) GetBucketId() string {
	return sm.bucketId
}
func (sm *manager) SetBucketId(s string) {
	sm.bucketId = s
}
func (sm *manager) GetAuthToken() string {
	return sm.authToken
}
func (sm *manager) SetAuthToken(s string) {
	sm.authToken = s
}
