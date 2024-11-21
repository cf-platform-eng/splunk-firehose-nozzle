package testing

import (
	"errors"
	"fmt"
	"github.com/cloudfoundry/go-cfclient/v3/resource"
	"net/url"
	"sync"
)

type AppClientMock struct {
	lock                    sync.RWMutex
	apps                    map[string]*resource.App
	n                       int
	listAppsCallCount       int
	appByGUIDCallCount      int
	getOrgByGUIDCallCount   int
	getSpaceByGUIDCallCount int
}

func NewAppClientMock(n int) *AppClientMock {
	apps := getApps(n)
	return &AppClientMock{
		apps: apps,
		n:    n,
	}
}

func (m *AppClientMock) AppByGuid(guid string) (*resource.App, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.appByGUIDCallCount++

	app, ok := m.apps[guid]
	if ok {
		return app, nil
	}
	return app, errors.New("No such app")
}

func (m *AppClientMock) ListApps() ([]*resource.App, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	m.listAppsCallCount++

	var apps []*resource.App
	for k := range m.apps {
		apps = append(apps, m.apps[k])
	}
	return apps, nil
}

func (m *AppClientMock) ListAppsByQueryWithLimits(query url.Values, totalPages int) ([]*resource.App, error) {
	return m.ListApps()
}

func (m *AppClientMock) GetSpaceByGuid(spaceGUID string) (*resource.Space, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.getSpaceByGUIDCallCount++

	var id int
	fmt.Sscanf(spaceGUID, "cf_space_id_%d", &id)

	return &resource.Space{
		Resource:      resource.Resource{GUID: spaceGUID},
		Name:          fmt.Sprintf("cf_space_name_%d", id),
		Relationships: &resource.SpaceRelationships{Organization: &resource.ToOneRelationship{Data: &resource.Relationship{GUID: fmt.Sprintf("cf_org_id_%d", id)}}},
	}, nil
}

func (m *AppClientMock) GetOrgByGuid(orgGUID string) (*resource.Organization, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.getOrgByGUIDCallCount++

	var id int
	fmt.Sscanf(orgGUID, "cf_org_id_%d", &id)

	return &resource.Organization{
		Name:     fmt.Sprintf("cf_org_name_%d", id),
		Resource: resource.Resource{GUID: orgGUID}}, nil
}

func (m *AppClientMock) CreateApp(appID, spaceID string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	app := resource.App{
		Resource:      resource.Resource{GUID: appID},
		Name:          appID,
		Relationships: resource.SpaceRelationship{Space: resource.ToOneRelationship{Data: &resource.Relationship{GUID: spaceID}}},
		Metadata:      &resource.Metadata{Labels: make(map[string]*string)},
	}

	m.apps[appID] = &app
}

func (m *AppClientMock) DeleteApp(appID string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.apps, appID)
}

func getApps(n int) map[string]*resource.App {
	apps := make(map[string]*resource.App, n)
	for i := 0; i < n; i++ {
		app := resource.App{
			Resource:      resource.Resource{GUID: fmt.Sprintf("cf_app_id_%d", i)},
			Name:          fmt.Sprintf("cf_app_name_%d", i),
			Relationships: resource.SpaceRelationship{Space: resource.ToOneRelationship{Data: &resource.Relationship{GUID: fmt.Sprintf("cf_space_id_%d", i%50)}}},
			Metadata:      &resource.Metadata{Labels: make(map[string]*string)},
		}
		apps[app.GUID] = &app
	}
	return apps
}

func (m *AppClientMock) ListAppsCallCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.listAppsCallCount
}

func (m *AppClientMock) AppByGUIDCallCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.appByGUIDCallCount
}

func (m *AppClientMock) GetOrgByGUIDCallCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getOrgByGUIDCallCount
}

func (m *AppClientMock) GetSpaceByGUIDCallCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getSpaceByGUIDCallCount
}

func (m *AppClientMock) ResetCallCounts() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.listAppsCallCount = 0
	m.appByGUIDCallCount = 0
	m.getOrgByGUIDCallCount = 0
	m.getSpaceByGUIDCallCount = 0
}
