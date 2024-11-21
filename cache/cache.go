package cache

import (
	"github.com/cloudfoundry/go-cfclient/v3/resource"
)

type App struct {
	Name        string
	Guid        string
	SpaceName   string
	SpaceGuid   string
	OrgName     string
	OrgGuid     string
	CfAppLabels map[string]*string
	IgnoredApp  bool
}

type Cache interface {
	Open() error
	Close() error
	GetAllApps() (map[string]*App, error)
	GetApp(string) (*App, error)
}

type AppClient interface {
	AppByGuid(appGuid string) (*resource.App, error)
	ListApps() ([]*resource.App, error)
	GetSpaceByGuid(spaceGUID string) (*resource.Space, error)
	GetOrgByGuid(orgGUID string) (*resource.Organization, error)
}
