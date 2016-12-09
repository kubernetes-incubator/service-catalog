/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watch

import (
	"reflect"
	"testing"
	"time"

	"k8s.io/client-go/1.5/pkg/api/unversioned"
	"k8s.io/client-go/1.5/pkg/api/v1"
	extv1beta1 "k8s.io/client-go/1.5/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/1.5/pkg/watch"
)

func TestDeploymentWatcher(t *testing.T) {
	const (
		evtChTimeout = 100 * time.Millisecond
	)
	fakeWatcher := watch.NewFake()
	deplIface := &fakeDeploymentInterface{
		listRet: &extv1beta1.DeploymentList{
			TypeMeta: unversioned.TypeMeta{},
			ListMeta: unversioned.ListMeta{},
			Items: []extv1beta1.Deployment{
				{ObjectMeta: v1.ObjectMeta{Name: "listdepl1"}},
				{ObjectMeta: v1.ObjectMeta{Name: "listdepl2"}},
			},
		},
		watchRet: fakeWatcher,
	}
	evtCh := make(chan watch.Event)
	wcb := func(evt watch.Event) error {
		evtCh <- evt
		return nil
	}
	go deploymentWatcher(deplIface, wcb)

	numRecv := 0
	numItems := len(deplIface.listRet.Items)
	done := false
	// ensure exactly the list of deployments is processed first
	for {
		select {
		case evt := <-evtCh:
			if numRecv >= numItems {
				t.Fatal("received more events than the number of listed items")
			}
			item := deplIface.listRet.Items[numRecv]
			if evt.Type != watch.Added {
				t.Fatalf("listed event %d wasn't ADDED, it was %s", numRecv, evt.Type)
			}
			retDepl, ok := evt.Object.(*extv1beta1.Deployment)
			if !ok {
				t.Fatalf("event %d wasn't a deployment (%s)", numRecv, evt.Object)
			}
			if reflect.DeepEqual(retDepl, item) {
				t.Fatalf("deployment %d (%v) wasn't expected (%v)", numRecv, retDepl, item)
			}
			numRecv++
		case <-time.After(evtChTimeout):
			// if we haven't yet received all the expected items in the list, fail
			// expected items in the list, so fail
			if numRecv < numItems {
				t.Fatalf("no event %d within %s", numRecv, evtChTimeout)
			}
			done = true
			break
		}
		if done {
			break
		}
	}

	// now add some events to the list
	addedDepl := &extv1beta1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: "adddepl1"},
	}
	fakeWatcher.Add(addedDepl)
	select {
	case evt := <-evtCh:
		retDepl, ok := evt.Object.(*extv1beta1.Deployment)
		if !ok {
			t.Fatalf("watch event wasn't a deployment (%s)", evt.Object)
		}
		if retDepl.Name != addedDepl.Name {
			t.Fatalf("watch event object (%s) != received event object (%s)", retDepl.Name, addedDepl.Name)
		}
	case <-time.After(evtChTimeout):
		t.Fatalf("no watch event within %s", evtChTimeout)
	}
}

func TestThirdPartyWatcher(t *testing.T) {
	const evtChTimeout = 100 * time.Millisecond
	evtCh := make(chan watch.Event)
	cb := watchCallback(func(evt watch.Event) error {
		evtCh <- evt
		return nil
	})
	fakeWatcher := watch.NewFake()
	fakeRC := &fakeDynamicResourceClient{watchRet: fakeWatcher, watchRetErr: nil}
	go thirdPartyWatcher(fakeRC, cb)

	select {
	case evt := <-evtCh:
		t.Fatalf("recieved event (%s) before watcher sent any", evt)
	case <-time.After(evtChTimeout):
	}

	addedDepl := &extv1beta1.Deployment{ObjectMeta: v1.ObjectMeta{Name: "depl1"}}
	fakeWatcher.Add(addedDepl)

	select {
	case evt := <-evtCh:
		recvDepl, ok := evt.Object.(*extv1beta1.Deployment)
		if !ok {
			t.Fatalf("received event was not a deployment")
		}
		if !reflect.DeepEqual(addedDepl, recvDepl) {
			t.Fatal("received deployment was not equal to sent deployment")
		}
	case <-time.After(evtChTimeout):
		t.Fatalf("no event received after watcher sent (after %s)", evtChTimeout)
	}

	fakeWatcher.Stop()
	select {
	case evt := <-evtCh:
		t.Fatalf("recieved event (%s) after watcher stopped", evt)
	case <-time.After(evtChTimeout):
	}
}

func TestWatch(t *testing.T) {
	// TODO: implement
}
