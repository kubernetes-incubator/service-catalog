/*
Copyright 2018 The Kubernetes Authors.

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

package plan

import (
	"bytes"

	"github.com/kubernetes-incubator/service-catalog/cmd/svcat/test"

	"github.com/kubernetes-incubator/service-catalog/cmd/svcat/command"
	"github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1beta1"
	"github.com/kubernetes-incubator/service-catalog/pkg/svcat"
	servicecatalog "github.com/kubernetes-incubator/service-catalog/pkg/svcat/service-catalog"
	"github.com/kubernetes-incubator/service-catalog/pkg/svcat/service-catalog/service-catalogfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Create command", func() {
	Describe("NewCreateCmd", func() {
		It("Builds and returns a cobra command", func() {
			cxt := &command.Context{}
			cmd := NewCreateCmd(cxt)
			Expect(*cmd).NotTo(BeNil())
			Expect(cmd.Use).To(Equal("plan [NAME] --from [EXISTING_NAME]"))
			Expect(cmd.Short).To(Equal("Copies an existing plan into a new user-defined cluster-scoped or namespace-scoped plan"))
			Expect(cmd.Example).To(ContainSubstring("svcat create plan newplan --from mysqldb"))
			Expect(cmd.Example).To(ContainSubstring("svcat create plan newplan --from mysqldb --scope cluster"))
			Expect(cmd.Example).To(ContainSubstring("svcat create plan newplan --from mysqldb --scope namespace --namespace newnamespace"))
			Expect(cmd.Aliases).To(Equal(0))

			fromFlag := cmd.Flags().Lookup("from")
			Expect(fromFlag).NotTo(BeNil())
			Expect(fromFlag).To(ContainSubstring("Name of an existing plan that will be copied (Required)"))

			scopeFlag := cmd.Flags().Lookup("scope")
			Expect(scopeFlag).NotTo(BeNil())
			Expect(scopeFlag).To(ContainSubstring("Name of an existing plan that will be copied (Required)"))

			namespaceFlag := cmd.Flags().Lookup("namespace")
			Expect(namespaceFlag).NotTo(BeNil())
			Expect(namespaceFlag).To(ContainSubstring("Name of an existing plan that will be copied (Required)"))
		})
	})
	Describe("Validate()", func() {
		It("errors if no argument is provided", func() {
			cmd := CreateCmd{
				Name: "",
				From: "plan",
			}
			err := cmd.Validate([]string{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(ContainSubstring("new plan name should be provided"))
		})
	})
	Describe("Run()", func() {
		It("Calls the CreatePlan method with the input for a cluster class and prints output", func() {
			planName := "newplan"
			existingPlanName := "existingplan"

			planToReturn := &v1beta1.ClusterServicePlan{
				ObjectMeta: v1.ObjectMeta{
					Name: planName,
				},
			}

			outputBuffer := &bytes.Buffer{}

			fakeApp, _ := svcat.NewApp(nil, nil, "default")
			fakeSDK := new(servicecatalogfakes.FakeSvcatClient)
			fakeSDK.CreatePlanReturns(planToReturn, nil)
			fakeApp.SvcatClient = fakeSDK
			cmd := CreateCmd{
				Namespaced: &command.Namespaced{Context: svcattest.NewContext(outputBuffer, fakeApp)},
				Scoped:     command.NewScoped(),
				Name:       planName,
				From:       existingPlanName,
			}
			cmd.Scope = servicecatalog.ClusterScope
			err := cmd.Run()

			Expect(err).NotTo(HaveOccurred())
			opts := fakeSDK.CreatePlanArgsForCall(0)
			Expect(opts.Name).To(Equal(planName))
			Expect(opts.From).To(Equal(existingPlanName))

			output := outputBuffer.String()
			Expect(output).To(ContainSubstring(planName))
		})
		It("Calls the CreatePlan method with input for a namespace class and prints output", func() {
			planName := "newplan"
			planNamespace := "test-ns"
			existingPlanName := "existingplan"

			planToReturn := &v1beta1.ServicePlan{
				ObjectMeta: v1.ObjectMeta{
					Name:      planName,
					Namespace: planNamespace,
				},
			}

			outputBuffer := &bytes.Buffer{}

			fakeApp, _ := svcat.NewApp(nil, nil, "default")
			fakeSDK := new(servicecatalogfakes.FakeSvcatClient)
			fakeSDK.CreateClassFromReturns(planToReturn, nil)
			fakeApp.SvcatClient = fakeSDK
			cmd := CreateCmd{
				Namespaced: &command.Namespaced{
					Context: svcattest.NewContext(outputBuffer, fakeApp)},
				Scoped: command.NewScoped(),
				Name:   planName,
				From:   existingPlanName,
			}
			cmd.Scope = servicecatalog.NamespaceScope
			cmd.Namespace = planNamespace
			err := cmd.Run()

			Expect(err).NotTo(HaveOccurred())
			opts := fakeSDK.CreatePlanArgsForCall(0)
			Expect(opts.Name).To(Equal(planName))
			Expect(opts.From).To(Equal(existingPlanName))
			Expect(opts.Scope).To(Equal(servicecatalog.NamespaceScope))
			Expect(opts.Namespace).To(Equal(planNamespace))

			output := outputBuffer.String()
			Expect(output).To(ContainSubstring(planName))
			Expect(output).To(ContainSubstring(planNamespace))
		})
	})

})