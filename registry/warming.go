// Runs a daemon to continuously warm the registry cache.
package registry

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/registry/cache"
)

// Refresh entries when they are within this duration of expiring. See
// also `expiry` in the cache package.
const refreshWhenExpiryWithin = 5 * time.Minute

// Request an update to the images in use, then look for new image
// tags, no more often than this
const askForNewImagesInterval = time.Minute

type Warmer struct {
	Logger        log.Logger
	ClientFactory ClientFactory
	Creds         Credentials
	Writer        cache.Writer
	Reader        cache.Reader
	Burst         int
}

type ImageCreds map[flux.ImageID]Credentials

// Continuously get the images to populate the cache with, and
// populate the cache with them.
func (w *Warmer) Loop(stop <-chan struct{}, wg *sync.WaitGroup, imagesToFetchFunc func() ImageCreds) {
	defer wg.Done()

	if w.Logger == nil || w.ClientFactory == nil || w.Writer == nil || w.Reader == nil {
		panic("registry.Warmer fields are nil")
	}

	for k, v := range imagesToFetchFunc() {
		w.warm(k, v)
	}

	newImages := time.Tick(askForNewImagesInterval)
	for {
		select {
		case <-stop:
			w.Logger.Log("stopping", "true")
			return
		case <-newImages:
			for k, v := range imagesToFetchFunc() {
				w.warm(k, v)
			}
		}
	}
}

func (w *Warmer) warm(id flux.ImageID, creds Credentials) {
	client, err := w.ClientFactory.ClientFor(id.Host, creds)
	if err != nil {
		w.Logger.Log("err", err.Error())
		return
	}
	defer client.Cancel()

	username := w.Creds.credsFor(id.Host).username

	// Refresh tags first
	// Only, for example, "library/alpine" because we have the host information in the client above.
	tags, err := client.Tags(id)
	if err != nil {
		if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) && !strings.Contains(err.Error(), "net/http: request canceled") {
			w.Logger.Log("err", errors.Wrap(err, "requesting tags"))
		}
		return
	}

	val, err := json.Marshal(tags)
	if err != nil {
		w.Logger.Log("err", errors.Wrap(err, "serializing tags to store in cache"))
		return
	}

	key, err := cache.NewTagKey(username, id)
	if err != nil {
		w.Logger.Log("err", errors.Wrap(err, "creating key for cache"))
		return
	}

	err = w.Writer.SetKey(key, val)
	if err != nil {
		w.Logger.Log("err", errors.Wrap(err, "storing tags in cache"))
		return
	}

	// Create a list of manifests that need updating
	var expiring []flux.ImageID
	var missing []flux.ImageID
	for _, tag := range tags {
		// See if we have the manifest already cached
		// We don't want to re-download a manifest again.
		i := id.WithNewTag(tag)
		key, err := cache.NewManifestKey(username, i)
		if err != nil {
			w.Logger.Log("err", errors.Wrap(err, "creating key for memcache"))
			continue
		}
		expiry, err := w.Reader.GetExpiration(key)
		switch {
		case err == cache.ErrNotCached:
			missing = append(missing, i)
		case err == nil: // We've already got it
			if time.Until(expiry) > refreshWhenExpiryWithin {
				break
			}
			expiring = append(expiring, i)
		default:
			w.Logger.Log("err", err)
		}
	}

	if len(expiring)+len(missing) > 0 {
		w.Logger.Log("fetching", id.HostNamespaceImage(), "total", len(tags), "expiring", len(expiring), "missing", len(missing))
		toUpdate := append(missing, expiring...)

		// The upper bound for concurrent fetches against a single host is
		// w.Burst, so limit the number of fetching goroutines to that.
		fetchers := make(chan struct{}, w.Burst)
		awaitFetchers := &sync.WaitGroup{}
		successfullyUpdated := 0
		for _, imID := range toUpdate {
			awaitFetchers.Add(1)
			fetchers <- struct{}{}
			go func(imageID flux.ImageID) {
				defer func() { awaitFetchers.Done(); <-fetchers }()
				// Get the image from the remote
				img, err := client.Manifest(imageID)
				if err != nil {
					if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) && !strings.Contains(err.Error(), "net/http: request canceled") {
						w.Logger.Log("err", errors.Wrap(err, "requesting manifests"))
					}
					return
				}

				key, err := cache.NewManifestKey(username, img.ID)
				if err != nil {
					w.Logger.Log("err", errors.Wrap(err, "creating key for memcache"))
					return
				}
				// Write back to memcache
				val, err := json.Marshal(img)
				if err != nil {
					w.Logger.Log("err", errors.Wrap(err, "serializing tag to store in cache"))
					return
				}
				err = w.Writer.SetKey(key, val)
				if err != nil {
					w.Logger.Log("err", errors.Wrap(err, "storing manifests in cache"))
					return
				}
				successfullyUpdated += 1
			}(imID)
		}
		awaitFetchers.Wait()
		w.Logger.Log("updated", id.HostNamespaceImage(), "count", successfullyUpdated)
	}
}
