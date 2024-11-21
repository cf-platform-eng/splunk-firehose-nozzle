package cache_test

import (
	"fmt"
	"os"
	"time"

	"code.cloudfoundry.org/lager/v3"

	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/cache"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cache", func() {
	var (
		boltdbPath         = "/tmp/boltdb"
		ignoreMissingApps  = true
		appCacheTTL        = 2 * time.Second
		missingAppCacheTTL = 2 * time.Second
		orgSpaceCacheTTL   = 2 * time.Second
		n                  = 10

		nilApp *App = nil

		config = &BoltdbConfig{
			Path:               boltdbPath,
			IgnoreMissingApps:  ignoreMissingApps,
			AppCacheTTL:        appCacheTTL,
			MissingAppCacheTTL: missingAppCacheTTL,
			OrgSpaceCacheTTL:   orgSpaceCacheTTL,
			Logger:             lager.NewLogger("test"),
			AppLimits:          n,
		}

		client *testing.AppClientMock = nil
		cache  *Boltdb                = nil
		gerr   error                  = nil
	)

	BeforeEach(func() {
		os.Remove(boltdbPath)
		client = testing.NewAppClientMock(n)
		cache, gerr = NewBoltdb(client, config)
		Ω(gerr).ShouldNot(HaveOccurred())

		gerr = cache.Open()
		Ω(gerr).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		gerr = cache.Close()
		Ω(gerr).ShouldNot(HaveOccurred())

		time.Sleep(1 * time.Second)
		gerr = os.Remove(boltdbPath)
		Ω(gerr == nil || os.IsNotExist(gerr)).Should(BeTrue())
		// Ω(gerr).ShouldNot(HaveOccurred())
	})

	Context("Get app good case", func() {
		It("Have 10 apps", func() {
			apps, err := cache.GetAllApps()
			Ω(err).ShouldNot(HaveOccurred())

			Expect(apps).NotTo(Equal(nil))
			Expect(len(apps)).To(Equal(n))
		})

		It("Expect app", func() {
			guid := "cf_app_id_0"
			app, err := cache.GetApp(guid)
			Ω(err).ShouldNot(HaveOccurred())

			Expect(app).NotTo(Equal(nil))
			Expect(app.Guid).To(Equal(guid))
		})
	})

	Context("Get app bad case", func() {
		It("Expect no app", func() {
			guid := fmt.Sprintf("cf_app_id_not_exists_%d", time.Now().UnixNano())
			app, err := cache.GetApp(guid)
			Ω(err).Should(HaveOccurred())
			Expect(app).To(Equal(nilApp))

			// We ignore missing apps, so for the second time query, we already
			// recorded the missing app, so nil, err is expected to return
			app, err = cache.GetApp(guid)
			Ω(err).Should(HaveOccurred())
			Expect(err).To(Equal(ErrMissingAndIgnored))
			Expect(app).To(Equal(nilApp))

			time.Sleep(missingAppCacheTTL + 3)

			// We ignore missing apps, so for the 3rd time query after sleep,
			// the missing app cache will be cleaned up, so a not found error
			// will be returned instead of MissingAndIgnoredErr
			app, err = cache.GetApp(guid)
			Ω(err).Should(HaveOccurred())
			Expect(err).NotTo(Equal(ErrMissingAndIgnored))
			Expect(app).To(Equal(nilApp))
		})
	})

	Context("When orphan app is requested", func() {

		It("Should found app in cache", func() {
			app_guid := "orphan_app_id"
			client.CreateApp(app_guid, "orphan_space_id")
			Ω(cache.GetApp(app_guid)).NotTo(Equal(nil))
			client.DeleteApp(app_guid)
			cache.ManuallyInvalidateCaches()

			app, err := cache.GetApp(app_guid)
			Ω(err).ShouldNot(HaveOccurred())
			Expect(app).NotTo(Equal(nil))
			Expect(app.Guid).To(Equal(app_guid))
		})
	})

	Context("Cache invalidation", func() {
		BeforeEach(func() {
			// close the cache created in the outer BeforeEach
			cache.Close()

			config.AppCacheTTL = 0
			config.OrgSpaceCacheTTL = time.Second
			cache, gerr = NewBoltdb(client, config)
			Ω(gerr).ShouldNot(HaveOccurred())

			gerr = cache.Open()
			Ω(gerr).ShouldNot(HaveOccurred())
		})
		It("Expect new app", func() {
			now := time.Now().UnixNano()
			id := fmt.Sprintf("id_%d", now)
			client.CreateApp(id, fmt.Sprintf("cf_space_id_%d", now))

			client.ResetCallCounts()

			cache.ManuallyInvalidateCaches()

			app, err := cache.GetApp(id)
			Ω(err).ShouldNot(HaveOccurred())
			Expect(app).NotTo(Equal(nilApp))
			Expect(app.Guid).To(Equal(id))

			Expect(app.SpaceGuid).NotTo(BeEmpty())
			Expect(app.SpaceName).NotTo(BeEmpty())
			Expect(client.GetSpaceByGUIDCallCount()).To(Equal(11))

			Expect(app.OrgGuid).NotTo(BeEmpty())
			Expect(app.OrgName).NotTo(BeEmpty())
			Expect(client.GetOrgByGUIDCallCount()).To(Equal(11))

			apps, err := cache.GetAllApps()
			Ω(err).ShouldNot(HaveOccurred())

			Expect(apps).NotTo(Equal(nil))
			Expect(len(apps)).To(Equal(n + 1))
		})
	})

	Context("App Cache Invalidation but not Org/Space Cache Invalidation", func() {
		var (
			config *BoltdbConfig
			cache  *Boltdb
			client *testing.AppClientMock
		)

		BeforeEach(func() {
			boltdbPath := "/tmp/boltdb2"
			config = &BoltdbConfig{
				Path:               boltdbPath,
				IgnoreMissingApps:  ignoreMissingApps,
				AppCacheTTL:        appCacheTTL,
				MissingAppCacheTTL: missingAppCacheTTL,
				OrgSpaceCacheTTL:   48 * time.Hour,
				Logger:             lager.NewLogger("test"),
			}

			client = testing.NewAppClientMock(n)

			os.Remove(boltdbPath)
			cache, gerr = NewBoltdb(client, config)
			Ω(gerr).ShouldNot(HaveOccurred())

			gerr = cache.Open()
			Ω(gerr).ShouldNot(HaveOccurred())
		})

		It("Expects new app but no org space calls", func() {
			now := time.Now().UnixNano()
			id := fmt.Sprintf("id_%d", now)
			client.CreateApp(id, fmt.Sprintf("cf_space_id_%d", now))

			client.ResetCallCounts()

			// wait for the cache to invalidate and repopulate
			time.Sleep(appCacheTTL + (250 * time.Millisecond))

			app, err := cache.GetApp(id)
			Ω(err).ShouldNot(HaveOccurred())
			Expect(app).NotTo(Equal(nilApp))
			Expect(app.Guid).To(Equal(id))

			Expect(app.SpaceGuid).NotTo(BeEmpty())
			Expect(app.SpaceName).NotTo(BeEmpty())

			Expect(app.OrgGuid).NotTo(BeEmpty())
			Expect(app.OrgName).NotTo(BeEmpty())

			// this will be 1 because `invalidateCache` will have been called between ResetCallCounts and now but the org and space cache has not reached its TTL
			Expect(client.GetSpaceByGUIDCallCount()).To(Equal(1))
			Expect(client.GetOrgByGUIDCallCount()).To(Equal(1))

			apps, err := cache.GetAllApps()
			Ω(err).ShouldNot(HaveOccurred())

			Expect(apps).NotTo(Equal(nil))
			Expect(len(apps)).To(Equal(n + 1))
		})
	})

	Context("NewBoltdb error", func() {
		It("Expect error", func() {
			dup := *config
			dup.Path = fmt.Sprintf("/not-exists-%d/boltdb", time.Now().UnixNano())
			bcache, err := NewBoltdb(client, &dup)
			Ω(err).ShouldNot(HaveOccurred())

			err = bcache.Open()
			Ω(err).Should(HaveOccurred())
		})
	})

	Context("Load from existing boltdb", func() {
		It("Expect 10 apps from existing boltdb", func() {
			dup := *config
			dup.Path = fmt.Sprintf("/tmp/%d", time.Now().UnixNano())
			bcache, err := NewBoltdb(client, &dup)
			Ω(err).ShouldNot(HaveOccurred())

			err = bcache.Open()
			Ω(err).ShouldNot(HaveOccurred())

			defer os.Remove(dup.Path)
			time.Sleep(time.Second)
			bcache.Close()

			// Load from existing db
			bcache, err = NewBoltdb(client, &dup)
			Ω(err).ShouldNot(HaveOccurred())

			err = bcache.Open()
			Ω(err).ShouldNot(HaveOccurred())

			apps, err := bcache.GetAllApps()
			Ω(err).ShouldNot(HaveOccurred())

			Expect(apps).NotTo(Equal(nil))
			Expect(len(apps)).To(Equal(n))
		})
	})

	Context("No cache", func() {
		It("No error", func() {
			c := NewNoCache()
			err := c.Open()
			Ω(err).ShouldNot(HaveOccurred())

			apps, err := c.GetAllApps()
			Ω(err).ShouldNot(HaveOccurred())
			Expect(apps).To(BeNil())

			app, err := c.GetApp("testing")
			Ω(err).ShouldNot(HaveOccurred())
			Expect(app).To(BeNil())

			err = c.Close()
			Ω(err).ShouldNot(HaveOccurred())
		})
	})

})
