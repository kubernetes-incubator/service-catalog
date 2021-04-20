/*
Copyright 2017 The Kubernetes Authors.

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

package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	osb "github.com/kubernetes-sigs/go-open-service-broker-client/v2"
	fakeosb "github.com/kubernetes-sigs/go-open-service-broker-client/v2/fake"
	"sigs.k8s.io/yaml"

	"github.com/kubernetes-sigs/service-catalog/pkg/apis/servicecatalog/v1beta1"
	servicecataloginformers "github.com/kubernetes-sigs/service-catalog/pkg/client/informers_generated/externalversions"
	v1beta1informers "github.com/kubernetes-sigs/service-catalog/pkg/client/informers_generated/externalversions/servicecatalog/v1beta1"
	"github.com/kubernetes-sigs/service-catalog/pkg/util"

	servicecatalogclientset "github.com/kubernetes-sigs/service-catalog/pkg/client/clientset_generated/clientset/fake"
	"github.com/kubernetes-sigs/service-catalog/test/fake"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgofake "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

// NOTE:
//
// This file contains:
//
// - tests for the methods on controller.go
// - test fixtures used in other controller_*_test.go files
//
// Other controller_*_test.go files contain tests related to the reconciliation
// loops for the different catalog API resources.

const (
	testClusterServiceClassGUID            = "cscguid"
	testClusterServicePlanGUID             = "cspguid"
	testNonbindableClusterServiceClassGUID = "unbindable-clusterserviceclass"
	testNonbindableClusterServicePlanGUID  = "unbindable-clusterserviceplan"
	testServiceInstanceGUID                = "iguid"
	testServiceBindingGUID                 = "bguid"
	testNamespaceGUID                      = "test-ns-uid"
	testRemovedClusterServiceClassGUID     = "removed-clusterserviceclass"
	testRemovedClusterServicePlanGUID      = "removed-clusterserviceplan"

	testClusterServiceBrokerName            = "test-clusterservicebroker"
	testClusterServiceClassName             = "test-clusterserviceclass"
	testClusterServicePlanName              = "test-clusterserviceplan"
	testNonExistentClusterServiceClassName  = "nothere"
	testNonExistentClusterServicePlanName   = "nothere"
	testNonExistentServicePlanName          = "nothere"
	testNonbindableClusterServiceClassName  = "test-unbindable-clusterserviceclass"
	testNonbindableClusterServicePlanName   = "test-unbindable-clusterserviceplan"
	testRemovedClusterServiceClassName      = "removed-test-clusterserviceclass"
	testRemovedClusterServicePlanName       = "removed-test-clusterserviceplan"
	testServiceInstanceName                 = "test-instance"
	testServiceBindingName                  = "test-binding"
	testServiceBindingSecretName            = "test-secret"
	testServiceBindingSecretKey             = "test-secret-key"
	testNamespace                           = "test-ns"
	testServiceInstanceCredentialSecretName = "test-secret"
	testOperation                           = "test-operation"
	testClusterID                           = "test-cluster-id"
)

var (
	emptyServiceClasses = make(map[string]*v1beta1.ClusterServiceClass)
	emptyServicePlans   = make(map[string]*v1beta1.ClusterServicePlan)
	testDashboardURL    = "http://dashboard"
	testContext         = map[string]interface{}{
		"platform":           ContextProfilePlatformKubernetes,
		"namespace":          testNamespace,
		"instance_name":      testServiceInstanceName,
		clusterIdentifierKey: testClusterID,
	}
)

const testCatalog = `{
  "services": [{
    "name": "fake-service",
    "id": "acb56d7c-xxxx-xxxx-xxxx-feb140a59a66",
    "description": "fake service",
    "tags": ["no-sql", "relational"],
    "requires": ["route_forwarding"],
    "max_db_per_node": 5,
    "bindable": true,
    "metadata": {
      "provider": {
        "name": "The name"
      },
      "listing": {
        "imageUrl": "http://example.com/cat.gif",
        "blurb": "Add a blurb here",
        "longDescription": "A long time ago, in a galaxy far far away..."
      },
      "displayName": "The Fake ClusterServiceBroker"
    },
    "dashboard_client": {
      "id": "398e2f8e-xxxx-xxxx-xxxx-19a71ecbcf64",
      "secret": "277cabb0-xxxx-xxxx-xxxx-7822c0a90e5d",
      "redirect_uri": "http://localhost:1234"
    },
    "plan_updateable": true,
    "plans": [{
      "name": "fake-plan-1",
      "id": "d3031751-xxxx-xxxx-xxxx-a42377d3320e",
      "description": "Shared fake Server, 5tb persistent disk, 40 max concurrent connections",
      "max_storage_tb": 5,
      "metadata": {
        "costs":[
            {
               "amount":{
                  "usd":99.0
               },
               "unit":"MONTHLY"
            },
            {
               "amount":{
                  "usd":0.99
               },
               "unit":"1GB of messages over 20GB"
            }
         ],
        "bullets": [
            "Shared fake server",
            "5 TB storage",
            "40 concurrent connections"
        ]
      }
    }, {
      "name": "fake-plan-2",
      "id": "0f4008b5-xxxx-xxxx-xxxx-dace631cd648",
      "description": "Shared fake Server, 5tb persistent disk, 40 max concurrent connections. 100 async",
      "max_storage_tb": 5,
      "metadata": {
        "costs":[
            {
               "amount":{
                  "usd":199.0
               },
               "unit":"MONTHLY"
            },
            {
               "amount":{
                  "usd":0.99
               },
               "unit":"1GB of messages over 20GB"
            }
         ],
        "bullets": [
          "40 concurrent connections"
        ]
      }
    }]
  }]
}`

const alphaParameterSchemaCatalogBytes = `{
  "services": [{
    "name": "fake-service",
    "id": "acb56d7c-xxxx-xxxx-xxxx-feb140a59a66",
    "description": "fake service",
    "tags": ["tag1", "tag2"],
    "requires": ["route_forwarding"],
    "bindable": true,
    "metadata": {
    	"a": "b",
    	"c": "d"
    },
    "dashboard_client": {
      "id": "398e2f8e-xxxx-xxxx-xxxx-19a71ecbcf64",
      "secret": "277cabb0-xxxx-xxxx-xxxx-7822c0a90e5d",
      "redirect_uri": "http://localhost:1234"
    },
    "plan_updateable": true,
    "plans": [{
      "name": "fake-plan-1",
      "id": "d3031751-xxxx-xxxx-xxxx-a42377d3320e",
      "description": "description1",
      "metadata": {
      	"b": "c",
      	"d": "e"
      },
      "schemas": {
      	"service_instance": {
	  	  "create": {
	  	  	"parameters": {
	          "$schema": "http://json-schema.org/draft-04/schema",
	          "type": "object",
	          "title": "Parameters",
	          "properties": {
	            "name": {
	              "title": "Queue Name",
	              "type": "string",
	              "maxLength": 63,
	              "default": "My Queue"
	            },
	            "email": {
	              "title": "Email",
	              "type": "string",
	              "pattern": "^\\S+@\\S+$",
	              "description": "Email address for alerts."
	            },
	            "protocol": {
	              "title": "Protocol",
	              "type": "string",
	              "default": "Java Message Service (JMS) 1.1",
	              "enum": [
	                "Java Message Service (JMS) 1.1",
	                "Transmission Control Protocol (TCP)",
	                "Advanced Message Queuing Protocol (AMQP) 1.0"
	              ]
	            },
	            "secure": {
	              "title": "Enable security",
	              "type": "boolean",
	              "default": true
	            }
	          },
	          "required": [
	            "name",
	            "protocol"
	          ]
	  	  	}
	  	  },
	  	  "update": {
	  	  	"parameters": {
	  		  "baz": "zap"
	  	    }
	  	  }
      	},
      	"service_binding": {
      	  "create": {
	  	  	"parameters": {
      	  	  "zoo": "blu"
      	    }
      	  }
      	}
      }
    }]
  }]
}`

const instanceParameterSchemaBytes = `{
  "$schema": "http://json-schema.org/draft-04/schema",
  "type": "object",
  "title": "Parameters",
  "properties": {
    "name": {
      "title": "Queue Name",
      "type": "string",
      "maxLength": 63,
      "default": "My Queue"
    },
    "email": {
      "title": "Email",
      "type": "string",
      "pattern": "^\\S+@\\S+$",
      "description": "Email address for alerts."
    },
    "protocol": {
      "title": "Protocol",
      "type": "string",
      "default": "Java Message Service (JMS) 1.1",
      "enum": [
        "Java Message Service (JMS) 1.1",
        "Transmission Control Protocol (TCP)",
        "Advanced Message Queuing Protocol (AMQP) 1.0"
      ]
    },
    "secure": {
      "title": "Enable security",
      "type": "boolean",
      "default": true
    }
  },
  "required": [
    "name",
    "protocol"
  ]
}`

const largeTestCatalog = `
{
  "services": [
    {
      "id": "41726368-6f6e-4569-8172-63686f6e6569",
      "name": "Archonei",
      "description": "The contents of my chamber pot are more able than Ser Harys.",
      "requires": [
        "SeaBitch"
      ],
      "bindable": true,
      "plans": [
        {
          "id": "476f6c64-656e-4772-af76-65476f6c6465",
          "name": "Goldengrove",
          "description": "A reader lives a thousand lives before he dies. The man who never reads lives only one. The a mind needs books as a sword needs a whetstone, if it is to keep its edge.",
          "free": false,
          "metadata": {
            "CasterlyRock": "Crakehall",
            "Nightsong": "CrowsNest",
            "UnnamedBaelishcastle": "Feastfires"
          }
        }
      ],
      "dashboard_client": {
        "id": "41726368-6f6e-4569-a964-417263686f6e",
        "secret": "41726368-6f6e-4569-b365-637265744172",
        "redirect_uri": "http://localhost:1234"
      },
      "metadata": {
        "Ironoaks": "Crakehall",
        "Pyke": "ThreeTowers"
      }
    },
    {
      "id": "41727261-7841-4272-a178-417272617841",
      "name": "Arrax",
      "description": "Darkness will be your cloak, your shield, your mother's milk. Darkness will make you strong.",
      "requires": [
        "Woe"
      ],
      "bindable": true,
      "plans": [
        {
          "id": "45617374-7761-4463-a82d-62792d746865",
          "name": "Eastwatch-by-the-Sea",
          "description": "A reader lives a thousand lives before he dies. The man who never reads lives only one. The a mind needs books as a sword needs a whetstone, if it is to keep its edge.",
          "free": true,
          "metadata": {
            "CasterlyRock": "CrowsNest",
            "Nightsong": "Feastfires"
          }
        },
        {
          "id": "4f6c644f-616b-4f6c-a44f-616b4f6c644f",
          "name": "OldOak",
          "description": "The drapes kept out the dust and heat of the streets, but they could not keep out disappointment.",
          "free": true,
          "metadata": {
            "Highgarden": "Blackhaven",
            "Nightsong": "Whitewalls"
          }
        }
      ],
      "dashboard_client": {
        "id": "41727261-7869-4441-b272-617869644172",
        "secret": "41727261-7873-4563-b265-744172726178",
        "redirect_uri": "http://localhost:1234"
      }
    },
    {
      "id": "42616c65-7269-4f6e-8261-6c6572696f6e",
      "name": "Balerion",
      "description": "A reader lives a thousand lives before he dies. The man who never reads lives only one. The a mind needs books as a sword needs a whetstone, if it is to keep its edge.",
      "bindable": true,
      "plans": [
        {
          "id": "49726f6e-7261-4468-8972-6f6e72617468",
          "name": "Ironrath",
          "description": "A reader lives a thousand lives before he dies. The man who never reads lives only one. The a mind needs books as a sword needs a whetstone, if it is to keep its edge.",
          "free": false
        },
        {
          "id": "51756565-6e73-4761-b465-517565656e73",
          "name": "Queensgate",
          "description": "A man will win one tourney, and fall quickly in the next. A slick spot in the grass may mean defeat, or what you ate for supper the night before. A change in the wind may bring the gift of victory.",
          "free": true,
          "metadata": {
            "Runestone": "GhostHill"
          }
        }
      ],
      "dashboard_client": {
        "id": "42616c65-7269-4f6e-a964-42616c657269",
        "secret": "42616c65-7269-4f6e-b365-637265744261",
        "redirect_uri": "http://localhost:1234"
      },
      "metadata": {
        "CastleCerwyn": "Queensgate",
        "VulturesRoost": "StormsEnd"
      }
    }
  ]
}`

type testTimeoutError struct{}

func (e testTimeoutError) Error() string {
	return "timed out"
}

func (e testTimeoutError) Timeout() bool {
	return true
}

func getTestTimeoutError() error {
	return testTimeoutError{}
}

type originatingIdentityTestCase struct {
	name                        string
	includeUserInfo             bool
	enableOriginatingIdentity   bool
	expectedOriginatingIdentity bool
}

var originatingIdentityTestCases = []originatingIdentityTestCase{
	{
		name:                        "originating identity not included when feature disabled",
		includeUserInfo:             true,
		enableOriginatingIdentity:   false,
		expectedOriginatingIdentity: false,
	},
	{
		name:                        "originating identity not included when no creating user info",
		includeUserInfo:             false,
		enableOriginatingIdentity:   true,
		expectedOriginatingIdentity: false,
	},
	{
		name:                        "originating identity included",
		includeUserInfo:             true,
		enableOriginatingIdentity:   true,
		expectedOriginatingIdentity: true,
	},
}

var testUserInfo = &v1beta1.UserInfo{
	Username: "fakeusername",
	UID:      "fakeuid",
	Groups:   []string{"fakegroup1"},
	Extra: map[string]v1beta1.ExtraValue{
		"fakekey": v1beta1.ExtraValue([]string{"fakevalue"}),
	},
}

const testOriginatingIdentityValue = `{
	"username": "fakeusername",
	"uid": "fakeuid",
	"groups": ["fakegroup1"],
        "extra": {"fakekey": ["fakevalue"]}
}`

var testOriginatingIdentity = &osb.OriginatingIdentity{
	Platform: originatingIdentityPlatform,
	Value:    testOriginatingIdentityValue,
}

// broker used in most of the tests that need a broker
func getTestClusterServiceBroker() *v1beta1.ClusterServiceBroker {
	return &v1beta1.ClusterServiceBroker{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterServiceBrokerName},
		Spec: v1beta1.ClusterServiceBrokerSpec{
			CommonServiceBrokerSpec: v1beta1.CommonServiceBrokerSpec{
				URL:            "https://example.com",
				RelistBehavior: v1beta1.ServiceBrokerRelistBehaviorDuration,
				RelistDuration: &metav1.Duration{Duration: 15 * time.Minute},
			},
		},
	}
}

func getTestClusterServiceBrokerWithStatus(status v1beta1.ConditionStatus) *v1beta1.ClusterServiceBroker {
	t := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	return getTestClusterServiceBrokerWithStatusAndTime(status, t, t)
}

func getTestClusterServiceBrokerWithStatusAndTime(status v1beta1.ConditionStatus, lastTransitionTime, lastRelistTime metav1.Time) *v1beta1.ClusterServiceBroker {
	broker := getTestClusterServiceBroker()
	broker.Status = v1beta1.ClusterServiceBrokerStatus{
		CommonServiceBrokerStatus: v1beta1.CommonServiceBrokerStatus{
			Conditions: []v1beta1.ServiceBrokerCondition{{
				Type:               v1beta1.ServiceBrokerConditionReady,
				Status:             status,
				LastTransitionTime: lastTransitionTime,
			}},
			LastCatalogRetrievalTime: &lastRelistTime,
		},
	}
	return broker
}

func getTestClusterServiceBrokerWithAuth(authInfo *v1beta1.ClusterServiceBrokerAuthInfo) *v1beta1.ClusterServiceBroker {
	broker := getTestClusterServiceBroker()
	broker.Spec.AuthInfo = authInfo
	return broker
}

func getTestClusterBrokerBasicAuthInfo() *v1beta1.ClusterServiceBrokerAuthInfo {
	return &v1beta1.ClusterServiceBrokerAuthInfo{
		Basic: &v1beta1.ClusterBasicAuthConfig{
			SecretRef: &v1beta1.ObjectReference{Namespace: "test-ns", Name: "auth-secret"},
		},
	}
}

func getTestClusterBrokerBearerAuthInfo() *v1beta1.ClusterServiceBrokerAuthInfo {
	return &v1beta1.ClusterServiceBrokerAuthInfo{
		Bearer: &v1beta1.ClusterBearerTokenAuthConfig{
			SecretRef: &v1beta1.ObjectReference{Namespace: "test-ns", Name: "auth-secret"},
		},
	}
}

func getTestBasicAuthSecret() *corev1.Secret {
	return &corev1.Secret{
		Data: map[string][]byte{
			v1beta1.BasicAuthUsernameKey: []byte("foo"),
			v1beta1.BasicAuthPasswordKey: []byte("bar"),
		},
	}
}

func getTestBearerAuthSecret() *corev1.Secret {
	return &corev1.Secret{
		Data: map[string][]byte{
			v1beta1.BearerTokenKey: []byte("token"),
		},
	}
}

// a bindable service class wired to the result of getTestClusterServiceBroker()
func getTestClusterServiceClass() *v1beta1.ClusterServiceClass {
	broker := getTestClusterServiceBroker()
	class := &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testClusterServiceClassGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceClassRefName: util.GenerateSHA(testClusterServiceClassGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecExternalName:               util.GenerateSHA(testClusterServiceClassName),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName:   util.GenerateSHA(testClusterServiceBrokerName),
			},
		},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:  "a test service",
				ExternalName: testClusterServiceClassName,
				ExternalID:   testClusterServiceClassGUID,
				Bindable:     true,
			},
		},
	}
	markAsServiceCatalogManagedResource(class, broker)
	return class
}

func getTestClusterServiceClassWithoutLabels() *v1beta1.ClusterServiceClass {
	broker := getTestClusterServiceBroker()
	class := &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testClusterServiceClassGUID,
		},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:  "a test service",
				ExternalName: testClusterServiceClassName,
				ExternalID:   testClusterServiceClassGUID,
				Bindable:     true,
			},
		},
	}
	markAsServiceCatalogManagedResource(class, broker)
	return class
}

func getTestMarkedAsRemovedClusterServiceClass() *v1beta1.ClusterServiceClass {
	broker := getTestClusterServiceBroker()
	class := &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testRemovedClusterServiceClassGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceClassRefName: testClusterServiceClassGUID,
				v1beta1.GroupName + "/" + v1beta1.FilterSpecExternalName:               testClusterServiceClassName,
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName:   testClusterServiceBrokerName,
			},
		},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:  "a test service that has been marked as removed",
				ExternalName: testRemovedClusterServiceClassName,
				ExternalID:   testRemovedClusterServiceClassGUID,
				Bindable:     true,
			},
		},
		Status: v1beta1.ClusterServiceClassStatus{
			CommonServiceClassStatus: v1beta1.CommonServiceClassStatus{
				RemovedFromBrokerCatalog: true,
			},
		},
	}
	markAsServiceCatalogManagedResource(class, broker)
	return class
}

func getTestRemovedClusterServiceClass() *v1beta1.ClusterServiceClass {
	broker := getTestClusterServiceBroker()
	class := &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testRemovedClusterServiceClassGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName: util.GenerateSHA(testClusterServiceBrokerName),
			},
		},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:  "a test service that should be removed",
				ExternalName: testRemovedClusterServiceClassName,
				ExternalID:   testRemovedClusterServiceClassGUID,
				Bindable:     true,
			},
		},
	}
	markAsServiceCatalogManagedResource(class, broker)
	return class
}

func getTestBindingRetrievableClusterServiceClass() *v1beta1.ClusterServiceClass {
	return &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterServiceClassGUID},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:        "a test service",
				ExternalName:       testClusterServiceClassName,
				ExternalID:         testClusterServiceClassGUID,
				BindingRetrievable: true,
				Bindable:           true,
			},
		},
	}
}

func getTestBindingRetrievableServiceClass() *v1beta1.ServiceClass {
	return &v1beta1.ServiceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testServiceClassGUID,
			Namespace: testNamespace,
		},
		Spec: v1beta1.ServiceClassSpec{
			ServiceBrokerName: testServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				Description:        "a test service",
				ExternalName:       testServiceClassName,
				ExternalID:         testServiceClassGUID,
				BindingRetrievable: true,
				Bindable:           true,
			},
		},
	}
}

func getTestClusterServicePlan() *v1beta1.ClusterServicePlan {
	broker := getTestClusterServiceBroker()
	plan := &v1beta1.ClusterServicePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testClusterServicePlanGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServicePlanRefName:  util.GenerateSHA(testClusterServicePlanGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecExternalName:               util.GenerateSHA(testClusterServicePlanName),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName:   util.GenerateSHA(testClusterServiceBrokerName),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceClassRefName: util.GenerateSHA(testClusterServiceClassGUID),
			},
		},
		Spec: v1beta1.ClusterServicePlanSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
				ExternalID:   testClusterServicePlanGUID,
				ExternalName: testClusterServicePlanName,
				Bindable:     truePtr(),
			},
			ClusterServiceClassRef: v1beta1.ClusterObjectReference{
				Name: testClusterServiceClassGUID,
			},
		},
		Status: v1beta1.ClusterServicePlanStatus{},
	}
	markAsServiceCatalogManagedResource(plan, broker)
	return plan
}

func getTestMarkedAsRemovedClusterServicePlan() *v1beta1.ClusterServicePlan {
	broker := getTestClusterServiceBroker()
	plan := &v1beta1.ClusterServicePlan{
		ObjectMeta: metav1.ObjectMeta{Name: testRemovedClusterServicePlanGUID},
		Spec: v1beta1.ClusterServicePlanSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
				ExternalID:   testRemovedClusterServicePlanGUID,
				ExternalName: testRemovedClusterServicePlanName,
				Bindable:     truePtr(),
			},
			ClusterServiceClassRef: v1beta1.ClusterObjectReference{
				Name: testClusterServiceClassGUID,
			},
		},
		Status: v1beta1.ClusterServicePlanStatus{
			CommonServicePlanStatus: v1beta1.CommonServicePlanStatus{
				RemovedFromBrokerCatalog: true,
			},
		},
	}
	markAsServiceCatalogManagedResource(plan, broker)
	return plan
}

func getTestRemovedClusterServicePlan() *v1beta1.ClusterServicePlan {
	broker := getTestClusterServiceBroker()
	plan := &v1beta1.ClusterServicePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testRemovedClusterServicePlanGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecExternalName:               util.GenerateSHA(testRemovedClusterServicePlanGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceClassRefName: util.GenerateSHA(testClusterServiceClassGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName:   util.GenerateSHA(testClusterServiceBrokerName),
			},
		},
		Spec: v1beta1.ClusterServicePlanSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
				ExternalID:   testRemovedClusterServicePlanGUID,
				ExternalName: testRemovedClusterServicePlanName,
				Bindable:     truePtr(),
			},
			ClusterServiceClassRef: v1beta1.ClusterObjectReference{
				Name: testClusterServiceClassGUID,
			},
		},
	}
	markAsServiceCatalogManagedResource(plan, broker)
	return plan
}

func getTestClusterServicePlanNonbindable() *v1beta1.ClusterServicePlan {
	broker := getTestClusterServiceBroker()
	plan := &v1beta1.ClusterServicePlan{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNonbindableClusterServicePlanGUID,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServicePlanRefName: util.GenerateSHA(testNonbindableClusterServicePlanGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecExternalName:              util.GenerateSHA(testClusterServicePlanName),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceBrokerName:  util.GenerateSHA(testClusterServiceBrokerName),
			},
		},
		Spec: v1beta1.ClusterServicePlanSpec{
			CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
				ExternalName: testNonbindableClusterServicePlanName,
				ExternalID:   testNonbindableClusterServicePlanGUID,
				Bindable:     falsePtr(),
			},
			ClusterServiceClassRef: v1beta1.ClusterObjectReference{
				Name: testClusterServiceClassGUID,
			},
		},
	}
	markAsServiceCatalogManagedResource(plan, broker)
	return plan
}

// an unbindable service class wired to the result of getTestClusterServiceBroker()
func getTestNonbindableClusterServiceClass() *v1beta1.ClusterServiceClass {
	return &v1beta1.ClusterServiceClass{
		ObjectMeta: metav1.ObjectMeta{Name: testNonbindableClusterServiceClassGUID},
		Spec: v1beta1.ClusterServiceClassSpec{
			ClusterServiceBrokerName: testClusterServiceBrokerName,
			CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
				ExternalName: testNonbindableClusterServiceClassName,
				ExternalID:   testNonbindableClusterServiceClassGUID,
				Bindable:     false,
			},
		},
	}
}

// broker catalog that provides the service class named in of
// getTestClusterServiceClass()
func getTestCatalog() *osb.CatalogResponse {
	return &osb.CatalogResponse{
		Services: []osb.Service{
			{
				Name:        testClusterServiceClassName,
				ID:          testClusterServiceClassGUID,
				Description: "a test service",
				Bindable:    true,
				Plans: []osb.Plan{
					{
						Name:        testClusterServicePlanName,
						Free:        truePtr(),
						ID:          testClusterServicePlanGUID,
						Description: "a test plan",
					},
					{
						Name:        testNonbindableClusterServicePlanName,
						Free:        truePtr(),
						ID:          testNonbindableClusterServicePlanGUID,
						Description: "a test plan",
						Bindable:    falsePtr(),
					},
				},
			},
		},
	}
}

// broker catalog that provides the service class named in of
// getTestNamespacedServiceClass()
func getTestNamespacedCatalog() *osb.CatalogResponse {
	return &osb.CatalogResponse{
		Services: []osb.Service{
			{
				Name:        testServiceClassName,
				ID:          testServiceClassGUID,
				Description: "a test service",
				Bindable:    true,
				Plans: []osb.Plan{
					{
						Name:        testServicePlanName,
						Free:        truePtr(),
						ID:          testServicePlanGUID,
						Description: "a test plan",
					},
					{
						Name:        testNonbindableServicePlanName,
						Free:        truePtr(),
						ID:          testNonbindableServicePlanGUID,
						Description: "a test plan",
						Bindable:    falsePtr(),
					},
				},
			},
		},
	}
}

// instance referencing the result of getTestClusterServiceClass()
// and getTestClusterServicePlan()
// This version sets:
// ClusterServiceClassExternalName and ClusterServicePlanExternalName as well
// as ClusterServiceClassRef and ClusterServicePlanRef which means that the
// ClusterServiceClass and ClusterServicePlan are fetched using
// Service[Class|Plan]Lister.get(spec.Service[Class|Plan]Ref.Name)
func getTestServiceInstanceWithClusterRefs() *v1beta1.ServiceInstance {
	sc := getTestServiceInstance()
	sc.Spec.ClusterServiceClassRef = &v1beta1.ClusterObjectReference{Name: testClusterServiceClassGUID}
	sc.Spec.ClusterServicePlanRef = &v1beta1.ClusterObjectReference{Name: testClusterServicePlanGUID}
	return sc
}

func getTestServiceInstanceWithNamespacedRefs() *v1beta1.ServiceInstance {
	sc := getTestServiceInstanceNamespacedPlanRef()
	sc.Spec.ServiceClassRef = &v1beta1.LocalObjectReference{Name: testServiceClassGUID}
	sc.Spec.ServicePlanRef = &v1beta1.LocalObjectReference{Name: testServicePlanGUID}
	return sc
}

func getTestServiceInstanceWithRefsAndExternalProperties() *v1beta1.ServiceInstance {
	sc := getTestServiceInstanceWithClusterRefs()
	sc.Status.ExternalProperties = &v1beta1.ServiceInstancePropertiesState{
		ClusterServicePlanExternalID:   testClusterServicePlanGUID,
		ClusterServicePlanExternalName: testClusterServicePlanName,
	}
	return sc
}

// instance referencing the result of getTestClusterServiceClass()
// and getTestClusterServicePlan()
// This version sets:
// ClusterServiceClassExternalName and ClusterServicePlanExternalName, so depending on the
// test, you may need to add reactors that deal with List due to the need
// to resolve Names to IDs for both ClusterServiceClass and ClusterServicePlan
func getTestServiceInstance() *v1beta1.ServiceInstance {
	return &v1beta1.ServiceInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceInstanceName,
			Namespace:  testNamespace,
			Generation: 1,
			Labels: map[string]string{
				v1beta1.GroupName + "/" + v1beta1.FilterSpecServiceClassRefName:        util.GenerateSHA(testServiceClassGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecServicePlanRefName:         util.GenerateSHA(testServicePlanGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServiceClassRefName: util.GenerateSHA(testClusterServiceClassGUID),
				v1beta1.GroupName + "/" + v1beta1.FilterSpecClusterServicePlanRefName:  util.GenerateSHA(testClusterServicePlanGUID),
			},
		},
		Spec: v1beta1.ServiceInstanceSpec{
			PlanReference: v1beta1.PlanReference{
				ClusterServiceClassExternalName: testClusterServiceClassName,
				ClusterServicePlanExternalName:  testClusterServicePlanName,
			},
			ExternalID: testServiceInstanceGUID,
		},
		Status: v1beta1.ServiceInstanceStatus{
			DeprovisionStatus: v1beta1.ServiceInstanceDeprovisionStatusRequired,
		},
	}
}

// instance referencing the result of getTestServiceClass()
// and getTestServicePlan()
// This version sets:
// ServiceClassExternalName and ServicePlanExternalName, so depending on the
// test, you may need to add reactors that deal with List due to the need
// to resolve Names to IDs for both ServiceClass and ServicePlan
func getTestServiceInstanceNamespacedPlanRef() *v1beta1.ServiceInstance {
	return &v1beta1.ServiceInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceInstanceName,
			Namespace:  testNamespace,
			Generation: 1,
		},
		Spec: v1beta1.ServiceInstanceSpec{
			PlanReference: v1beta1.PlanReference{
				ServiceClassExternalName: testServiceClassName,
				ServicePlanExternalName:  testServicePlanName,
			},
			ExternalID: testServiceInstanceGUID,
		},
		Status: v1beta1.ServiceInstanceStatus{
			DeprovisionStatus: v1beta1.ServiceInstanceDeprovisionStatusRequired,
		},
	}
}

// instance referencing the result of getTestClusterServiceClass()
// and getTestClusterServicePlan()
// This version sets:
// ClusterServiceClassName and ClusterServicePlanName
func getTestServiceInstanceK8SNames() *v1beta1.ServiceInstance {
	return &v1beta1.ServiceInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceInstanceName,
			Namespace:  testNamespace,
			Generation: 1,
		},
		Spec: v1beta1.ServiceInstanceSpec{
			PlanReference: v1beta1.PlanReference{
				ClusterServiceClassName: testClusterServiceClassGUID,
				ClusterServicePlanName:  testClusterServicePlanGUID,
			},
			ExternalID: testServiceInstanceGUID,
		},
		Status: v1beta1.ServiceInstanceStatus{
			Conditions:        []v1beta1.ServiceInstanceCondition{},
			DeprovisionStatus: v1beta1.ServiceInstanceDeprovisionStatusNotRequired,
		},
	}
}

// an instance referencing the result of getTestNonbindableClusterServiceClass, on the non-bindable plan.
func getTestNonbindableServiceInstance() *v1beta1.ServiceInstance {
	i := getTestServiceInstance()
	i.Spec.ClusterServiceClassExternalName = testNonbindableClusterServiceClassName
	i.Spec.ClusterServicePlanExternalName = testNonbindableClusterServicePlanName
	i.Spec.ClusterServiceClassRef = &v1beta1.ClusterObjectReference{Name: testNonbindableClusterServiceClassGUID}
	i.Spec.ClusterServicePlanRef = &v1beta1.ClusterObjectReference{Name: testNonbindableClusterServicePlanGUID}

	return i
}

// an instance referencing the result of getTestNonbindableClusterServiceClass, on the bindable plan.
func getTestServiceInstanceNonbindableServiceBindablePlan() *v1beta1.ServiceInstance {
	i := getTestNonbindableServiceInstance()
	i.Spec.ClusterServicePlanExternalName = testClusterServicePlanName
	i.Spec.ClusterServicePlanRef = &v1beta1.ClusterObjectReference{Name: testClusterServicePlanGUID}

	return i
}

func getTestServiceInstanceBindableServiceNonbindablePlan() *v1beta1.ServiceInstance {
	i := getTestServiceInstanceWithClusterRefs()
	i.Spec.ClusterServicePlanExternalName = testNonbindableClusterServicePlanName
	i.Spec.ClusterServicePlanRef = &v1beta1.ClusterObjectReference{Name: testNonbindableClusterServicePlanGUID}

	return i
}

func getTestServiceInstanceWithStatus(status v1beta1.ConditionStatus) *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithRefsAndExternalProperties()
	instance.Status.Conditions = []v1beta1.ServiceInstanceCondition{{
		Type:               v1beta1.ServiceInstanceConditionReady,
		Status:             status,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
	}}
	return instance
}

func getTestServiceInstanceWithFailedStatus() *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()
	instance.Status = v1beta1.ServiceInstanceStatus{
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:   v1beta1.ServiceInstanceConditionFailed,
			Status: v1beta1.ConditionTrue,
		}},
	}

	return instance
}

func getTestServiceInstanceUpdatingPlan() *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()
	instance.Generation = 2
	instance.Status = v1beta1.ServiceInstanceStatus{
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:   v1beta1.ServiceInstanceConditionReady,
			Status: v1beta1.ConditionTrue,
		}},
		ExternalProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: "old-plan-name",
			ClusterServicePlanExternalID:   "old-plan-id",
		},
		// It's been provisioned successfully.
		ReconciledGeneration: 1,
		ObservedGeneration:   1,
		ProvisionStatus:      v1beta1.ServiceInstanceProvisionStatusProvisioned,
		DeprovisionStatus:    v1beta1.ServiceInstanceDeprovisionStatusRequired,
	}

	return instance
}

func getTestServiceInstanceUpdatingParametersOfDeletedPlan() *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()
	instance.Generation = 2
	instance.Status = v1beta1.ServiceInstanceStatus{
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:   v1beta1.ServiceInstanceConditionReady,
			Status: v1beta1.ConditionTrue,
		}},
		ExternalProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: testRemovedClusterServicePlanName,
			ClusterServicePlanExternalID:   testRemovedClusterServicePlanGUID,
		},
		// It's been provisioned successfully.
		ReconciledGeneration: 1,
		ObservedGeneration:   1,
		ProvisionStatus:      v1beta1.ServiceInstanceProvisionStatusProvisioned,
		DeprovisionStatus:    v1beta1.ServiceInstanceDeprovisionStatusRequired,
	}

	return instance
}

// getTestServiceInstanceAsync returns an instance in async mode
func getTestServiceInstanceAsyncProvisioning(operation string) *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	instance.Status = v1beta1.ServiceInstanceStatus{
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:               v1beta1.ServiceInstanceConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Provisioning",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		}},
		AsyncOpInProgress:  true,
		OperationStartTime: &operationStartTime,
		CurrentOperation:   v1beta1.ServiceInstanceOperationProvision,
		InProgressProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: testClusterServicePlanName,
			ClusterServicePlanExternalID:   testClusterServicePlanGUID,
		},
		ObservedGeneration: instance.Generation,
		DeprovisionStatus:  v1beta1.ServiceInstanceDeprovisionStatusRequired,
	}
	if operation != "" {
		instance.Status.LastOperation = &operation
	}

	return instance
}

// getTestServiceInstanceAsyncUpdating returns an instance for which there is an
// in-progress async update
func getTestServiceInstanceAsyncUpdating(operation string) *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()
	instance.Generation = 2

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	instance.Status = v1beta1.ServiceInstanceStatus{
		ReconciledGeneration: 1,
		ObservedGeneration:   2,
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:               v1beta1.ServiceInstanceConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Updating",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		}},
		AsyncOpInProgress:  true,
		OperationStartTime: &operationStartTime,
		CurrentOperation:   v1beta1.ServiceInstanceOperationUpdate,
		InProgressProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: testClusterServicePlanName,
			ClusterServicePlanExternalID:   testClusterServicePlanGUID,
		},
		ExternalProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: "old-plan-name",
			ClusterServicePlanExternalID:   "old-plan-id",
		},
		ProvisionStatus:   v1beta1.ServiceInstanceProvisionStatusProvisioned,
		DeprovisionStatus: v1beta1.ServiceInstanceDeprovisionStatusRequired,
	}
	if operation != "" {
		instance.Status.LastOperation = &operation
	}

	return instance
}

func getTestServiceInstanceAsyncDeprovisioning(operation string) *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceWithClusterRefs()
	instance.Generation = 2

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	instance.Status = v1beta1.ServiceInstanceStatus{
		Conditions: []v1beta1.ServiceInstanceCondition{{
			Type:               v1beta1.ServiceInstanceConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Deprovisioning",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		}},
		AsyncOpInProgress:  true,
		OperationStartTime: &operationStartTime,
		CurrentOperation:   v1beta1.ServiceInstanceOperationDeprovision,
		InProgressProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: testClusterServicePlanName,
			ClusterServicePlanExternalID:   testClusterServicePlanGUID,
		},

		ReconciledGeneration: 1,
		ObservedGeneration:   2,
		ExternalProperties: &v1beta1.ServiceInstancePropertiesState{
			ClusterServicePlanExternalName: testClusterServicePlanName,
			ClusterServicePlanExternalID:   testClusterServicePlanGUID,
		},
		ProvisionStatus:   v1beta1.ServiceInstanceProvisionStatusProvisioned,
		DeprovisionStatus: v1beta1.ServiceInstanceDeprovisionStatusRequired,
	}
	if operation != "" {
		instance.Status.LastOperation = &operation
	}

	// Set the deleted timestamp to simulate deletion
	ts := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	instance.DeletionTimestamp = &ts
	return instance
}

func getTestServiceInstanceAsyncDeprovisioningWithFinalizer(operation string) *v1beta1.ServiceInstance {
	instance := getTestServiceInstanceAsyncDeprovisioning(operation)
	instance.ObjectMeta.Finalizers = []string{v1beta1.FinalizerServiceCatalog}
	return instance
}

// binding referencing the result of getTestServiceInstance()
func getTestServiceBinding() *v1beta1.ServiceBinding {
	return &v1beta1.ServiceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceBindingName,
			Namespace:  testNamespace,
			Finalizers: []string{v1beta1.FinalizerServiceCatalog},
			Generation: 1,
		},
		Spec: v1beta1.ServiceBindingSpec{
			SecretName:  testServiceBindingSecretName,
			InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
			ExternalID:  testServiceBindingGUID,
		},
		Status: v1beta1.ServiceBindingStatus{
			UnbindStatus: v1beta1.ServiceBindingUnbindStatusRequired,
		},
	}
}

// instantiates a yet-to-be-created binding
func getTestServiceInactiveBinding() *v1beta1.ServiceBinding {
	return &v1beta1.ServiceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testServiceBindingName,
			Namespace:  testNamespace,
			Finalizers: []string{v1beta1.FinalizerServiceCatalog},
			Generation: 1,
		},
		Spec: v1beta1.ServiceBindingSpec{
			InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
			ExternalID:  testServiceBindingGUID,
		},
		Status: v1beta1.ServiceBindingStatus{
			Conditions:   []v1beta1.ServiceBindingCondition{},
			UnbindStatus: v1beta1.ServiceBindingUnbindStatusNotRequired,
		},
	}
}

func getTestServiceBindingUnbinding() *v1beta1.ServiceBinding {
	binding := &v1beta1.ServiceBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testServiceBindingName,
			Namespace:         testNamespace,
			DeletionTimestamp: &metav1.Time{},
			Finalizers:        []string{v1beta1.FinalizerServiceCatalog},
			Generation:        2,
		},
		Spec: v1beta1.ServiceBindingSpec{
			InstanceRef: v1beta1.LocalObjectReference{Name: testServiceInstanceName},
			ExternalID:  testServiceBindingGUID,
			SecretName:  testServiceBindingSecretName,
		},
		Status: v1beta1.ServiceBindingStatus{
			ReconciledGeneration: 1,
			ExternalProperties:   &v1beta1.ServiceBindingPropertiesState{},
			UnbindStatus:         v1beta1.ServiceBindingUnbindStatusRequired,
		},
	}

	return binding
}

func getTestServiceBindingWithFailedStatus() *v1beta1.ServiceBinding {
	binding := getTestServiceBinding()
	binding.Status = v1beta1.ServiceBindingStatus{
		Conditions: []v1beta1.ServiceBindingCondition{{
			Type:   v1beta1.ServiceBindingConditionFailed,
			Status: v1beta1.ConditionTrue,
		}},
		UnbindStatus: v1beta1.ServiceBindingUnbindStatusNotRequired,
	}

	return binding
}

// getTestServiceBindingAsyncBinding returns an instance credential in async mode
func getTestServiceBindingAsyncBinding(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBinding()

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	binding.Status = v1beta1.ServiceBindingStatus{
		Conditions: []v1beta1.ServiceBindingCondition{{
			Type:               v1beta1.ServiceBindingConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Binding",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		},
		},
		AsyncOpInProgress:    true,
		OperationStartTime:   &operationStartTime,
		CurrentOperation:     v1beta1.ServiceBindingOperationBind,
		InProgressProperties: &v1beta1.ServiceBindingPropertiesState{},
		UnbindStatus:         v1beta1.ServiceBindingUnbindStatusRequired,
	}
	if operation != "" {
		binding.Status.LastOperation = &operation
	}

	return binding
}

// getTestServiceBindingAsyncBinding returns a binding service binding in
// async mode whose retry duration has been exceeded
func getTestServiceBindingAsyncBindingRetryDurationExceeded(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBindingAsyncBinding(operation)
	var startTime = metav1.NewTime(time.Now().Add(-7 * 24 * time.Hour))
	binding.Status.OperationStartTime = &startTime
	return binding
}

// getTestServiceBindingAsyncUnbinding returns an unbinding service binding in
// async mode
func getTestServiceBindingAsyncUnbinding(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBinding()
	binding.Generation = 2

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))

	binding.Status = v1beta1.ServiceBindingStatus{
		Conditions: []v1beta1.ServiceBindingCondition{{
			Type:               v1beta1.ServiceBindingConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Unbinding",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		}},
		AsyncOpInProgress:    true,
		OperationStartTime:   &operationStartTime,
		CurrentOperation:     v1beta1.ServiceBindingOperationUnbind,
		ReconciledGeneration: 1,
		ExternalProperties:   &v1beta1.ServiceBindingPropertiesState{},
		UnbindStatus:         v1beta1.ServiceBindingUnbindStatusRequired,
	}
	if operation != "" {
		binding.Status.LastOperation = &operation
	}

	return binding
}

// getTestServiceBindingAsyncUnbinding returns an unbinding service binding in
// async mode whose retry duration has been exceeded
func getTestServiceBindingAsyncUnbindingRetryDurationExceeded(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBindingAsyncUnbinding(operation)
	var startTime = metav1.NewTime(time.Now().Add(-7 * 24 * time.Hour))
	binding.Status.OperationStartTime = &startTime
	return binding
}

// getTestServiceBindingAsyncUnbinding returns a service binding undergoing
// orphan mitigation in async mode
func getTestServiceBindingAsyncOrphanMitigation(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBinding()

	operationStartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))

	binding.Status = v1beta1.ServiceBindingStatus{
		Conditions: []v1beta1.ServiceBindingCondition{{
			Type:               v1beta1.ServiceBindingConditionReady,
			Status:             v1beta1.ConditionFalse,
			Message:            "Binding",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		}},
		AsyncOpInProgress:          true,
		OrphanMitigationInProgress: true,
		OperationStartTime:         &operationStartTime,
		CurrentOperation:           v1beta1.ServiceBindingOperationBind,
		ReconciledGeneration:       0,
		InProgressProperties:       &v1beta1.ServiceBindingPropertiesState{},
		UnbindStatus:               v1beta1.ServiceBindingUnbindStatusRequired,
	}
	if operation != "" {
		binding.Status.LastOperation = &operation
	}

	return binding
}

// getTestServiceBindingAsyncOrphanMitigation returns a service binding
// undergoing orphan mitigation in async mode whose retry duration has been
// exceeded
func getTestServiceBindingAsyncOrphanMitigationRetryDurationExceeded(operation string) *v1beta1.ServiceBinding {
	binding := getTestServiceBindingAsyncOrphanMitigation(operation)
	var startTime = metav1.NewTime(time.Now().Add(-7 * 24 * time.Hour))
	binding.Status.OperationStartTime = &startTime
	return binding
}

type instanceParameters struct {
	Name string            `json:"name"`
	Args map[string]string `json:"args"`
}

type bindingParameters struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

func TestEmptyCatalogConversion(t *testing.T) {
	serviceClasses, servicePlans, err := convertAndFilterCatalog(&osb.CatalogResponse{}, nil, emptyServiceClasses, emptyServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}
	if len(serviceClasses) != 0 {
		t.Fatalf("Expected 0 serviceclasses for empty catalog, but got: %d", len(serviceClasses))
	}
	if len(servicePlans) != 0 {
		t.Fatalf("Expected 0 serviceplans for empty catalog, but got: %d", len(servicePlans))
	}
}

func TestCatalogConversion(t *testing.T) {
	catalog := &osb.CatalogResponse{}
	err := json.Unmarshal([]byte(testCatalog), &catalog)
	if err != nil {
		t.Fatalf("Failed to unmarshal test catalog: %v", err)
	}
	serviceClasses, servicePlans, err := convertAndFilterCatalog(catalog, nil, emptyServiceClasses, emptyServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}
	if len(serviceClasses) != 1 {
		t.Fatalf("Expected 1 serviceclasses for testCatalog, but got: %d", len(serviceClasses))
	}
	if len(servicePlans) != 2 {
		t.Fatalf("Expected 2 plans for testCatalog, but got: %d", len(servicePlans))
	}

	checkPlan(servicePlans[0], "d3031751-xxxx-xxxx-xxxx-a42377d3320e", "d3031751-xxxx-xxxx-xxxx-a42377d3320e", "fake-plan-1", "Shared fake Server, 5tb persistent disk, 40 max concurrent connections", t)
	checkPlan(servicePlans[1], "0f4008b5-xxxx-xxxx-xxxx-dace631cd648", "0f4008b5-xxxx-xxxx-xxxx-dace631cd648", "fake-plan-2", "Shared fake Server, 5tb persistent disk, 40 max concurrent connections. 100 async", t)
}

func TestCatalogConversionWithPreexistingClassesAndPlans(t *testing.T) {
	catalog := &osb.CatalogResponse{}
	err := json.Unmarshal([]byte(testCatalog), &catalog)
	if err != nil {
		t.Fatalf("Failed to unmarshal test catalog: %v", err)
	}
	testClassExternalID := "foo-bar"
	testPlanExternalID := "foo-bar-plan"
	catalog.Services[0].ID = testClassExternalID
	catalog.Services[0].Plans[0].ID = testPlanExternalID

	oldServiceClasses := make(map[string]*v1beta1.ClusterServiceClass)
	oldServiceClass := getTestClusterServiceClass()
	oldServiceClass.Name = testClassExternalID
	oldServiceClass.Spec.ExternalID = testClassExternalID
	oldServiceClasses[catalog.Services[0].ID] = oldServiceClass

	oldServicePlans := make(map[string]*v1beta1.ClusterServicePlan)
	oldServicePlan := getTestClusterServicePlan()
	oldServiceClass.Spec.ExternalID = testPlanExternalID
	oldServicePlans[catalog.Services[0].Plans[0].ID] = oldServicePlan

	serviceClasses, servicePlans, err := convertAndFilterCatalog(catalog, nil, oldServiceClasses, oldServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}
	if len(serviceClasses) != 1 {
		t.Fatalf("Expected 1 serviceclasses for testCatalog, but got: %d", len(serviceClasses))
	}
	checkClass(serviceClasses[0], testClassExternalID, testClassExternalID, "fake-service", "fake service", t)

	if len(servicePlans) != 2 {
		t.Fatalf("Expected 2 plans for testCatalog, but got: %d", len(servicePlans))
	}
	checkPlan(servicePlans[0], oldServicePlan.Spec.ExternalID, testPlanExternalID, "fake-plan-1", "Shared fake Server, 5tb persistent disk, 40 max concurrent connections", t)
	checkPlan(servicePlans[1], "0f4008b5-xxxx-xxxx-xxxx-dace631cd648", "0f4008b5-xxxx-xxxx-xxxx-dace631cd648", "fake-plan-2", "Shared fake Server, 5tb persistent disk, 40 max concurrent connections. 100 async", t)
}

func TestCatalogConversionWithParameterSchemas(t *testing.T) {
	catalog := &osb.CatalogResponse{}
	err := json.Unmarshal([]byte(alphaParameterSchemaCatalogBytes), &catalog)
	if err != nil {
		t.Fatalf("Failed to unmarshal test catalog: %v", err)
	}
	serviceClasses, servicePlans, err := convertAndFilterCatalog(catalog, nil, emptyServiceClasses, emptyServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}
	if len(serviceClasses) != 1 {
		t.Fatalf("Expected 1 serviceclasses for testCatalog, but got: %d", len(serviceClasses))
	}
	if len(servicePlans) != 1 {
		t.Fatalf("Expected 1 plan for testCatalog, but got: %d", len(servicePlans))
	}

	plan := servicePlans[0]
	if plan.Spec.InstanceCreateParameterSchema == nil {
		t.Fatalf("Expected plan.InstanceCreateParameterSchema to be set, but was nil")
	}

	cSchema := make(map[string]interface{})
	if err := json.Unmarshal(plan.Spec.InstanceCreateParameterSchema.Raw, &cSchema); err == nil {
		schema := make(map[string]interface{})
		if err := json.Unmarshal([]byte(instanceParameterSchemaBytes), &schema); err != nil {
			t.Fatalf("Error unmarshalling schema bytes: %v", err)
		}

		if e, a := schema, cSchema; !reflect.DeepEqual(e, a) {
			t.Fatalf("Unexpected value of alphaInstanceCreateParameterSchema; expected %v, got %v", e, a)
		}
	}

	if plan.Spec.InstanceUpdateParameterSchema == nil {
		t.Fatalf("Expected plan.InstanceUpdateParameterSchema to be set, but was nil")
	}
	m := make(map[string]string)
	if err := json.Unmarshal(plan.Spec.InstanceUpdateParameterSchema.Raw, &m); err == nil {
		if e, a := "zap", m["baz"]; e != a {
			t.Fatalf("Unexpected value of alphaInstanceUpdateParameterSchema; expected %v, got %v", e, a)
		}
	}

	if plan.Spec.ServiceBindingCreateParameterSchema == nil {
		t.Fatalf("Expected plan.ServiceBindingCreateParameterSchema to be set, but was nil")
	}
	m = make(map[string]string)
	if err := json.Unmarshal(plan.Spec.ServiceBindingCreateParameterSchema.Raw, &m); err == nil {
		if e, a := "blu", m["zoo"]; e != a {
			t.Fatalf("Unexpected value of ServiceBindingCreateParameterSchema; expected %v, got %v", e, a)
		}
	}
}

func checkClass(class *v1beta1.ClusterServiceClass, classK8sName, classID, className, classDescription string, t *testing.T) {
	if class.Name != classK8sName {
		t.Errorf("Expected class name to be %q, but was: %q", classK8sName, class.Name)
	}
	if class.Spec.ExternalID != classID {
		t.Errorf("Expected class ExternalID to be %q, but was: %q", classID, class.Spec.ExternalID)
	}
	if class.Spec.ExternalName != className {
		t.Errorf("Expected plan ExternalName to be %q, but was: %q", className, class.Spec.ExternalName)
	}
	if class.Spec.Description != classDescription {
		t.Errorf("Expected class description to be %q, but was: %q", classDescription, class.Spec.Description)
	}
}

func checkPlan(plan *v1beta1.ClusterServicePlan, planK8sName, planID, planName, planDescription string, t *testing.T) {
	if plan.Name != planK8sName {
		t.Errorf("Expected plan name to be %q, but was: %q", planK8sName, plan.Name)
	}
	if plan.Spec.ExternalID != planID {
		t.Errorf("Expected plan ExternalID to be %q, but was: %q", planID, plan.Spec.ExternalID)
	}
	if plan.Spec.ExternalName != planName {
		t.Errorf("Expected plan ExternalName to be %q, but was: %q", planName, plan.Spec.ExternalName)
	}
	if plan.Spec.Description != planDescription {
		t.Errorf("Expected plan description to be %q, but was: %q", planDescription, plan.Spec.Description)
	}
}

// FIX: there is an inconsistency between the current broker API types re: the
// Service.Metadata field.  Our repo types it as `interface{}`, the go-open-
// service-broker-client types it as `map[string]interface{}`.
func TestCatalogConversionMultipleClusterServiceClasses(t *testing.T) {
	// catalog := &osb.CatalogResponse{}
	// err := json.Unmarshal([]byte(testCatalogWithMultipleServices), &catalog)
	// if err != nil {
	// 	t.Fatalf("Failed to unmarshal test catalog: %v", err)
	// }

	// serviceClasses, err := convertAndFilterCatalog(catalog)
	// if err != nil {
	// 	t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	// }
	// if len(serviceClasses) != 2 {
	// 	t.Fatalf("Expected 2 serviceclasses for empty catalog, but got: %d", len(serviceClasses))
	// }
	// foundSvcMeta1 := false
	// // foundSvcMeta2 := false
	// // foundPlanMeta := false
	// for _, sc := range serviceClasses {
	// 	// For service1 make sure we have service level metadata with field1 = value1 as the blob
	// 	// and for service1 plan s1plan2 we have planmeta = planvalue as the blob.
	// 	if sc.Name == "service1" {
	// 		if sc.Description != "service 1 description" {
	// 			t.Fatalf("Expected service1's description to be \"service 1 description\", but was: %s", sc.Description)
	// 		}
	// 		if sc.ExternalMetadata != nil && len(sc.ExternalMetadata.Raw) > 0 {
	// 			m := make(map[string]string)
	// 			if err := json.Unmarshal(sc.ExternalMetadata.Raw, &m); err == nil {
	// 				if m["field1"] == "value1" {
	// 					foundSvcMeta1 = true
	// 				}
	// 			}

	// 		}
	// 		if len(sc.Plans) != 2 {
	// 			t.Fatalf("Expected 2 plans for service1 but got: %d", len(sc.Plans))
	// 		}
	// 		for _, sp := range sc.Plans {
	// 			if sp.Name == "s1plan2" {
	// 				if sp.ExternalMetadata != nil && len(sp.ExternalMetadata.Raw) > 0 {
	// 					m := make(map[string]string)
	// 					if err := json.Unmarshal(sp.ExternalMetadata.Raw, &m); err != nil {
	// 						t.Fatalf("Failed to unmarshal plan metadata: %s: %v", string(sp.ExternalMetadata.Raw), err)
	// 					}
	// 					if m["planmeta"] == "planvalue" {
	// 						foundPlanMeta = true
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// 	// For service2 make sure we have service level metadata with three element array with elements
	// 	// "first", "second", and "third"
	// 	if sc.Name == "service2" {
	// 		if sc.Description != "service 2 description" {
	// 			t.Fatalf("Expected service2's description to be \"service 2 description\", but was: %s", sc.Description)
	// 		}
	// 		if sc.ExternalMetadata != nil && len(sc.ExternalMetadata.Raw) > 0 {
	// 			m := make([]string, 0)
	// 			if err := json.Unmarshal(sc.ExternalMetadata.Raw, &m); err != nil {
	// 				t.Fatalf("Failed to unmarshal service metadata: %s: %v", string(sc.ExternalMetadata.Raw), err)
	// 			}
	// 			if len(m) != 3 {
	// 				t.Fatalf("Expected 3 fields in metadata, but got %d", len(m))
	// 			}
	// 			foundFirst := false
	// 			foundSecond := false
	// 			foundThird := false
	// 			for _, e := range m {
	// 				if e == "first" {
	// 					foundFirst = true
	// 				}
	// 				if e == "second" {
	// 					foundSecond = true
	// 				}
	// 				if e == "third" {
	// 					foundThird = true
	// 				}
	// 			}
	// 			if !foundFirst {
	// 				t.Fatalf("Didn't find 'first' in plan metadata")
	// 			}
	// 			if !foundSecond {
	// 				t.Fatalf("Didn't find 'second' in plan metadata")
	// 			}
	// 			if !foundThird {
	// 				t.Fatalf("Didn't find 'third' in plan metadata")
	// 			}
	// 			foundSvcMeta2 = true
	// 		}
	// 	}
	// }
	// if !foundSvcMeta1 {
	// 	t.Fatalf("Didn't find metadata in service1")
	// }
	// if !foundSvcMeta2 {
	// 	t.Fatalf("Didn't find metadata in service2")
	// }
	// if !foundPlanMeta {
	// 	t.Fatalf("Didn't find metadata '' in service1 plan2")
	// }

}

func TestConvertAndFilterCatalog(t *testing.T) {
	cases := []struct {
		name         string
		restrictions *v1beta1.CatalogRestrictions
		classes      []string
		plans        []string
		catalog      string
		error        bool
	}{
		{
			name:    "no restriction",
			classes: []string{"Archonei", "Arrax", "Balerion"},
			plans:   []string{"Goldengrove", "Eastwatch-by-the-Sea", "OldOak", "Ironrath", "Queensgate"},
			catalog: largeTestCatalog,
		},
		{
			name: "bad predicate",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"bad predicate"},
			},
			catalog: largeTestCatalog,
			error:   true,
		},
		{
			name: "by externalName",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"spec.externalName=Archonei"},
			},
			classes: []string{"Archonei"},
			plans:   []string{"Goldengrove"},
			catalog: largeTestCatalog,
		},
		{
			name: "by name",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"name=41727261-7841-4272-a178-417272617841"},
			},
			classes: []string{"Arrax"},
			plans:   []string{"Eastwatch-by-the-Sea", "OldOak"},
			catalog: largeTestCatalog,
		},
		{
			name: "whitelist by externalName",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"spec.externalName in (Archonei, Arrax)"},
			},
			classes: []string{"Archonei", "Arrax"},
			plans:   []string{"Goldengrove", "Eastwatch-by-the-Sea", "OldOak"},
			catalog: largeTestCatalog,
		},
		{
			name: "blacklist by externalName",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"spec.externalName notin (Balerion)"},
			},
			classes: []string{"Archonei", "Arrax"},
			plans:   []string{"Goldengrove", "Eastwatch-by-the-Sea", "OldOak"},
			catalog: largeTestCatalog,
		},
		{
			name: "whitelist and trim services without plans",
			restrictions: &v1beta1.CatalogRestrictions{
				ServicePlan: []string{"spec.externalName in (Goldengrove)"},
			},
			classes: []string{"Archonei"},
			plans:   []string{"Goldengrove"},
			catalog: largeTestCatalog,
		},
		{
			name: "whitelist plans by name",
			restrictions: &v1beta1.CatalogRestrictions{
				ServicePlan: []string{"name in (476f6c64-656e-4772-af76-65476f6c6465, 45617374-7761-4463-a82d-62792d746865, 49726f6e-7261-4468-8972-6f6e72617468)"},
			},
			classes: []string{"Archonei", "Arrax", "Balerion"},
			plans:   []string{"Goldengrove", "Eastwatch-by-the-Sea", "Ironrath"},
			catalog: largeTestCatalog,
		},
		{
			name: "two restrictions for plans",
			restrictions: &v1beta1.CatalogRestrictions{
				ServicePlan: []string{"name!=45617374-7761-4463-a82d-62792d746865", "spec.externalName notin (OldOak)"},
			},
			classes: []string{"Archonei", "Balerion"},
			plans:   []string{"Goldengrove", "Ironrath", "Queensgate"},
			catalog: largeTestCatalog,
		},
		{
			name: "two valid but conflicting whitelists for classes",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"spec.externalName in (Archonei)", "spec.externalName notin (Archonei)"},
			},
			classes: []string{},
			plans:   []string{},
			catalog: largeTestCatalog,
		},
		{
			name: "restrictions for both class and plan",
			restrictions: &v1beta1.CatalogRestrictions{
				ServiceClass: []string{"spec.externalName in (Archonei, Balerion)"},
				ServicePlan:  []string{"spec.externalName notin (Ironrath)"},
			},
			classes: []string{"Archonei", "Balerion"},
			plans:   []string{"Goldengrove", "Queensgate"},
			catalog: largeTestCatalog,
		},
		{
			name: "filter free plans",
			restrictions: &v1beta1.CatalogRestrictions{
				ServicePlan: []string{"spec.free==true"},
			},
			classes: []string{"Arrax", "Balerion"},
			plans:   []string{"Eastwatch-by-the-Sea", "OldOak", "Queensgate"},
			catalog: largeTestCatalog,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &osb.CatalogResponse{}
			err := json.Unmarshal([]byte(tc.catalog), &catalog)
			if err != nil {
				t.Fatalf("Failed to unmarshal test catalog: %v", err)
			}
			classes, plans, err := convertAndFilterCatalog(catalog, tc.restrictions, emptyServiceClasses, emptyServicePlans)
			if err != nil {
				if tc.error {
					return
				}
				t.Fatalf("Failed to convertAndFilterCatalog: %v, %+v", err, tc.restrictions)
			}

			if len(classes) != len(tc.classes) {
				t.Errorf("Filtered catalog did not contained expected number of classes, %s", expectedGot(len(tc.classes), len(classes)))
			}
			for _, class := range classes {
				if !contains(tc.classes, class.Spec.ExternalName) {
					t.Errorf("Unexpected class in filtered catalog, %s", class.Spec.ExternalName)
				}
			}
			if len(plans) != len(tc.plans) {
				t.Errorf("Filtered catalog did not contained expected number of plans, %s", expectedGot(len(tc.plans), len(plans)))
			}
			for _, plan := range plans {
				if !contains(tc.plans, plan.Spec.ExternalName) {
					t.Errorf("Unexpected plan in filtered catalog, %s", plan.Spec.ExternalName)
				}
			}
		})
	}
}

func contains(list []string, c string) bool {
	for _, l := range list {
		if l == c {
			return true
		}
	}
	return false
}

func TestFilterServicePlans(t *testing.T) {

	cases := []struct {
		name         string
		requirements []string
		accepted     int
		rejected     int
		catalog      string
	}{
		{
			name:     "no restriction",
			accepted: 2,
			rejected: 0,
			catalog:  testCatalog,
		},
		{
			name:         "by external name",
			requirements: []string{"spec.externalName=fake-plan-1"},
			accepted:     1,
			rejected:     1,
			catalog:      testCatalog,
		},
		{
			name:         "by external name",
			requirements: []string{"spec.externalName=real-plan-1"},
			accepted:     0,
			rejected:     2,
			catalog:      testCatalog,
		},
	}

	for _, tc := range cases {
		testName := fmt.Sprintf("%s:%s", tc.name, tc.requirements)
		t.Run(testName, func(t *testing.T) {
			catalog := &osb.CatalogResponse{}
			err := json.Unmarshal([]byte(tc.catalog), &catalog)
			if err != nil {
				t.Fatalf("Failed to unmarshal test catalog: %v", err)
			}
			_, servicePlans, err := convertAndFilterCatalog(catalog, nil, emptyServiceClasses, emptyServicePlans)
			if err != nil {
				t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
			}
			total := tc.accepted + tc.rejected
			if len(servicePlans) != total {
				t.Fatalf("Catalog did not contained expected number of plans, %s", expectedGot(total, len(servicePlans)))
			}

			restrictions := &v1beta1.CatalogRestrictions{
				ServicePlan: tc.requirements,
			}

			acceptedServiceClass, rejectedServiceClasses, err := filterServicePlans(restrictions, servicePlans)
			if len(acceptedServiceClass) != tc.accepted {
				t.Fatalf("Unexpected number of accepted service plans after filtering, %s", expectedGot(tc.accepted, len(acceptedServiceClass)))
			}

			if len(rejectedServiceClasses) != tc.rejected {
				t.Fatalf("Unexpected number of accepted service plans after filtering, %s", expectedGot(tc.rejected, len(rejectedServiceClasses)))
			}
		})
	}
}

const testCatalogForClusterServicePlanBindableOverride = `{
  "services": [
    {
      "name": "bindable",
      "id": "bindable-id",
      "bindable": true,
      "plans": [{
        "name": "bindable-bindable",
        "id": "s1-plan1-id"
      },
      {
        "name": "bindable-unbindable",
        "id": "s1-plan2-id",
        "bindable": false
      }]
    },
    {
      "name": "unbindable",
      "id": "unbindable-id",
      "bindable": false,
      "plans": [{
        "name": "unbindable-unbindable",
        "id": "s2-plan1-id"
      },
      {
        "name": "unbindable-bindable",
        "id": "s2-plan2-id",
        "bindable": true
      }]
    }
]}`

func truePtr() *bool {
	b := true
	return &b
}

func falsePtr() *bool {
	b := false
	return &b
}

func strPtr(s string) *string {
	return &s
}

func TestCatalogConversionClusterServicePlanBindable(t *testing.T) {
	catalog := &osb.CatalogResponse{}
	err := json.Unmarshal([]byte(testCatalogForClusterServicePlanBindableOverride), &catalog)
	if err != nil {
		t.Fatalf("Failed to unmarshal test catalog: %v", err)
	}

	aclasses, aplans, err := convertAndFilterCatalog(catalog, nil, emptyServiceClasses, emptyServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}

	eclasses := []*v1beta1.ClusterServiceClass{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bindable-id",
			},
			Spec: v1beta1.ClusterServiceClassSpec{
				CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
					ExternalName: "bindable",
					ExternalID:   "bindable-id",
					Bindable:     true,
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "unbindable-id",
			},
			Spec: v1beta1.ClusterServiceClassSpec{
				CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
					ExternalName: "unbindable",
					ExternalID:   "unbindable-id",
					Bindable:     false,
				},
			},
		},
	}

	eplans := []*v1beta1.ClusterServicePlan{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s1-plan1-id",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalID:   "s1-plan1-id",
					ExternalName: "bindable-bindable",
					Bindable:     nil,
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "bindable-id",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s1-plan2-id",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "bindable-unbindable",
					ExternalID:   "s1-plan2-id",
					Bindable:     falsePtr(),
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "bindable-id",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s2-plan1-id",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "unbindable-unbindable",
					ExternalID:   "s2-plan1-id",
					Bindable:     nil,
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "unbindable-id",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s2-plan2-id",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "unbindable-bindable",
					ExternalID:   "s2-plan2-id",
					Bindable:     truePtr(),
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "unbindable-id",
				},
			},
		},
	}

	if !reflect.DeepEqual(eclasses, aclasses) {
		t.Errorf("Unexpected diff between expected and actual serviceclasses: %v", diff.ObjectReflectDiff(eclasses, aclasses))
	}
	if !reflect.DeepEqual(eplans, aplans) {
		t.Errorf("Unexpected diff between expected and actual serviceplans: %v", diff.ObjectReflectDiff(eplans, aplans))
	}

}

const testCatalogForClusterServiceClassAndPlanWithInvalidExternalIDCharacters = `{
  "services": [
    {
      "name": "mysql",
      "id": "mysqlclass-1234-A",
      "plans": [{
        "name": "normal-characters",
        "id": "mysql-100mb"
      },
      {
        "name": "contains-escape-chars",
        "id": "abz-class"
      },
      {
        "name": "invalid-internal-characters",
        "id": "invalid/characters"
      },
			{
				"name": "invalid-starting-ending-characters",
				"id": "InvalidstarT"
      },
			{
				"name": "starting-ending-periods",
				"id": ".start."
      },
			{
				"name": "period-next-to-invalid-character",
				"id": "foo.Bar"
      },
			{
				"name": "internal-period",
				"id": "foo.bar"
      },
			{
				"name": "too-long",
				"id": "thisnameisreallyreallywaytoolonghowcouldanyonepossiblyreadthisplansname"
      },
			{
				"name": "too-long-with-problem-chars",
				"id": "thisnameisreallyreallywaytool-onghowcouldanyonepossiblyreadthisplansname"
      },
			{
				"name": "too-long-with-problem-period",
				"id": "thisnameisreallyreallywaytool.onghowcouldanyonepossiblyreadthisplansname"
			}]
    }
]}`

func TestCatalogConversionInvalidCharactersInExternalID(t *testing.T) {
	catalog := &osb.CatalogResponse{}
	err := json.Unmarshal([]byte(testCatalogForClusterServiceClassAndPlanWithInvalidExternalIDCharacters), &catalog)
	if err != nil {
		t.Fatalf("Failed to unmarshal test catalog: %v", err)
	}

	aclasses, aplans, err := convertAndFilterCatalog(catalog, nil, emptyServiceClasses, emptyServicePlans)
	if err != nil {
		t.Fatalf("Failed to convertAndFilterCatalog: %v", err)
	}

	eclasses := []*v1beta1.ClusterServiceClass{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mysqlclass-1234-z41z",
			},
			Spec: v1beta1.ClusterServiceClassSpec{
				CommonServiceClassSpec: v1beta1.CommonServiceClassSpec{
					ExternalName: "mysql",
					ExternalID:   "mysqlclass-1234-A",
				},
			},
		},
	}

	eplans := []*v1beta1.ClusterServicePlan{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-100mb",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "normal-characters",
					ExternalID:   "mysql-100mb",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "abz7az-class",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "contains-escape-chars",
					ExternalID:   "abz-class",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "invalidz2fzcharacters",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "invalid-internal-characters",
					ExternalID:   "invalid/characters",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "z49znvalidstarz54z",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "invalid-starting-ending-characters",
					ExternalID:   "InvalidstarT",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "z2ezstartz2ez",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "starting-ending-periods",
					ExternalID:   ".start.",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "foo.z42zar",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "period-next-to-invalid-character",
					ExternalID:   "foo.Bar",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "foo.bar",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "internal-period",
					ExternalID:   "foo.bar",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "thisnameisreallyreallywaytoolo-3f54131bf8c96c7d946ee1d7a32136dd",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "too-long",
					ExternalID:   "thisnameisreallyreallywaytoolonghowcouldanyonepossiblyreadthisplansname",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "thisnameisreallyreallywaytool--ca868df5f4322294bd65bd7e81866660",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "too-long-with-problem-chars",
					ExternalID:   "thisnameisreallyreallywaytool-onghowcouldanyonepossiblyreadthisplansname",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "thisnameisreallyreallywaytool-fa00db817d4c467b3bf3893240ed04a1",
			},
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					ExternalName: "too-long-with-problem-period",
					ExternalID:   "thisnameisreallyreallywaytool.onghowcouldanyonepossiblyreadthisplansname",
				},
				ClusterServiceClassRef: v1beta1.ClusterObjectReference{
					Name: "mysqlclass-1234-z41z",
				},
			},
		},
	}

	if !reflect.DeepEqual(eclasses, aclasses) {
		t.Errorf("Unexpected diff between expected and actual serviceclasses: %v", diff.ObjectReflectDiff(eclasses, aclasses))
	}
	if !reflect.DeepEqual(eplans, aplans) {
		t.Errorf("Unexpected diff between expected and actual serviceplans: %v", diff.ObjectReflectDiff(eplans, aplans))
	}

}

func TestGenerateEscapedName(t *testing.T) {
	externalIDs := []string{
		"simple",
		"contains-escape-char-z",
		"ContainsCaps",
		".starts-and-ends-with-periods.",
		"-starts-and-ends-with-hyphens-",
		"adjacent.-specialchars",
		"otherorderadjacent-.specialchars",
		"waytoomany-.-.-.-.-adjacentspecialchars",
		"thisnameisreallyreallywaytoolonghowcouldanyonepossiblyreadthisplansname",
		"thisnameisreallyreallywaytool.onghowcouldanyonepossiblyreadthisplansnamewithproblemperiod",
	}
	eEscapedNames := []string{
		"simple",
		"contains-escape-char-z7az",
		"z43zontainsz43zaps",
		"z2ezstarts-and-ends-with-periodsz2ez",
		"z2dzstarts-and-ends-with-hyphensz2dz",
		"adjacent.z2dzspecialchars",
		"otherorderadjacent-z2ezspecialchars",
		"waytoomany-z2ez-z2ez-z2ez-z2ez-adjacentspecialchars",
		"thisnameisreallyreallywaytoolo-3f54131bf8c96c7d946ee1d7a32136dd",
		"thisnameisreallyreallywaytool-84b392d072a2cbe3a18f87e7f7776d94",
	}
	for i, v := range externalIDs {
		aEscapedName := GenerateEscapedName(v)
		if eEscapedNames[i] != aEscapedName {
			t.Errorf("Unexpected diff between expected and actual escaped name: %v", diff.ObjectReflectDiff(eEscapedNames[i], aEscapedName))
		}
	}
}

func TestIsClusterServiceBrokerReady(t *testing.T) {
	cases := []struct {
		name  string
		input *v1beta1.ServiceInstance
		ready bool
	}{
		{
			name:  "ready",
			input: getTestServiceInstanceWithStatus(v1beta1.ConditionTrue),
			ready: true,
		},
		{
			name:  "no status",
			input: getTestServiceInstance(),
			ready: false,
		},
		{
			name:  "not ready",
			input: getTestServiceInstanceWithStatus(v1beta1.ConditionFalse),
			ready: false,
		},
	}

	for _, tc := range cases {
		if e, a := tc.ready, isServiceInstanceReady(tc.input); e != a {
			t.Errorf("%v: expected result %v, got %v", tc.name, e, a)
		}
	}
}

func TestIsPlanBindable(t *testing.T) {
	serviceClass := func(bindable bool) *v1beta1.ClusterServiceClass {
		serviceClass := getTestClusterServiceClass()
		serviceClass.Spec.Bindable = bindable
		return serviceClass
	}

	servicePlan := func(bindable *bool) *v1beta1.ClusterServicePlan {
		return &v1beta1.ClusterServicePlan{
			Spec: v1beta1.ClusterServicePlanSpec{
				CommonServicePlanSpec: v1beta1.CommonServicePlanSpec{
					Bindable: bindable,
				},
			},
		}
	}

	cases := []struct {
		name         string
		serviceClass bool
		servicePlan  *bool
		bindable     bool
	}{
		{
			name:         "service true, plan not set",
			serviceClass: true,
			bindable:     true,
		},
		{
			name:         "service true, plan false",
			serviceClass: true,
			servicePlan:  falsePtr(),
			bindable:     false,
		},
		{
			name:         "service true, plan true",
			serviceClass: true,
			servicePlan:  truePtr(),
			bindable:     true,
		},
		{
			name:         "service false, plan not set",
			serviceClass: false,
			bindable:     false,
		},
		{
			name:         "service false, plan false",
			serviceClass: false,
			servicePlan:  falsePtr(),
			bindable:     false,
		},
		{
			name:         "service false, plan true",
			serviceClass: false,
			servicePlan:  truePtr(),
			bindable:     true,
		},
	}

	for _, tc := range cases {
		sc := serviceClass(tc.serviceClass)
		plan := servicePlan(tc.servicePlan)

		if e, a := tc.bindable, isClusterServicePlanBindable(sc, plan); e != a {
			t.Errorf("%v: unexpected result; expected %v, got %v", tc.name, e, a)
		}
	}
}

// newTestController creates a new test controller injected with fake clients
// and returns:
//
// - a fake kubernetes core api client
// - a fake service catalog api client
// - a fake osb client
// - a test controller
// - the shared informers for the service catalog v1beta1 api
//
// If there is an error, newTestController calls 'Fatal' on the injected
// testing.T.
func newTestController(t *testing.T, config fakeosb.FakeClientConfiguration) (
	*clientgofake.Clientset,
	*fake.Clientset,
	*fakeosb.FakeClient,
	*controller,
	v1beta1informers.Interface) {
	// create a fake kube client
	fakeKubeClient := &clientgofake.Clientset{}
	// create a fake sc client
	fakeCatalogClient := &fake.Clientset{Clientset: &servicecatalogclientset.Clientset{}}

	fakeOSBClient := fakeosb.NewFakeClient(config) // error should always be nil
	brokerClFunc := fakeosb.ReturnFakeClientFunc(fakeOSBClient)

	// create informers
	informerFactory := servicecataloginformers.NewSharedInformerFactory(fakeCatalogClient, 0)
	serviceCatalogSharedInformers := informerFactory.Servicecatalog().V1beta1()

	fakeRecorder := record.NewFakeRecorder(5)

	// create a test controller
	testController, err := NewController(
		fakeKubeClient,
		fakeCatalogClient.ServicecatalogV1beta1(),
		serviceCatalogSharedInformers.ClusterServiceBrokers(),
		serviceCatalogSharedInformers.ServiceBrokers(),
		serviceCatalogSharedInformers.ClusterServiceClasses(),
		serviceCatalogSharedInformers.ServiceClasses(),
		serviceCatalogSharedInformers.ServiceInstances(),
		serviceCatalogSharedInformers.ServiceBindings(),
		serviceCatalogSharedInformers.ClusterServicePlans(),
		serviceCatalogSharedInformers.ServicePlans(),
		brokerClFunc,
		24*time.Hour,
		osb.LatestAPIVersion().HeaderValue(),
		fakeRecorder,
		7*24*time.Hour,
		7*24*time.Hour,
		DefaultClusterIDConfigMapName,
		DefaultClusterIDConfigMapNamespace,
		60*time.Second,
	)

	if err != nil {
		t.Fatal(err)
	}

	if c, ok := testController.(*controller); ok {
		c.setClusterID(testClusterID)
		c.brokerClientManager.clients[NewClusterServiceBrokerKey(getTestClusterServiceBroker().Name)] = clientWithConfig{
			OSBClient: fakeOSBClient,
		}
		c.brokerClientManager.clients[NewServiceBrokerKey(getTestServiceBroker().Namespace, getTestServiceBroker().Name)] = clientWithConfig{
			OSBClient: fakeOSBClient,
		}
	}

	fakeKubeClient.AddReactor("get", "namespaces", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: testNamespace,
				UID:  testNamespaceGUID,
			},
		}, nil
	})

	return fakeKubeClient, fakeCatalogClient, fakeOSBClient, testController.(*controller), serviceCatalogSharedInformers
}

func getRecordedEvents(testController *controller) []string {
	source := testController.recorder.(*record.FakeRecorder).Events
	done := false
	events := []string{}
	for !done {
		select {
		case event := <-source:
			events = append(events, event)
		default:
			done = true
		}
	}
	return events
}

func assertNumEvents(t *testing.T, strings []string, number int) {
	if e, a := number, len(strings); e != a {
		fatalf(t, "Unexpected number of events: expected %v, got %v;\nevents: %+v", e, a, strings)
	}
}

func fatalf(t *testing.T, msg string, args ...interface{}) {
	t.Log(string(debug.Stack()))
	t.Fatalf(msg, args...)
}

func assertNumberOfActions(t *testing.T, actions []clientgotesting.Action, number int) {
	if e, a := number, len(actions); e != a {
		t.Logf("%+v\n", actions)
		fatalf(t, "Unexpected number of actions: expected %v, got %v;\nactions: %+v", e, a, actions)
	}
}

func assertGet(t *testing.T, action clientgotesting.Action, obj interface{}) {
	assertActionFor(t, action, "get", "" /* subresource */, obj)
}

func assertList(t *testing.T, action clientgotesting.Action, obj interface{}, listRestrictions clientgotesting.ListRestrictions) {
	assertActionFor(t, action, "list", "" /* subresource */, obj)
	// Cast is ok since in the method above it's checked to be ListAction
	assertListRestrictions(t, listRestrictions, action.(clientgotesting.ListAction).GetListRestrictions())
}

func assertCreate(t *testing.T, action clientgotesting.Action, obj interface{}) runtime.Object {
	return assertActionFor(t, action, "create", "" /* subresource */, obj)
}

func assertUpdate(t *testing.T, action clientgotesting.Action, obj interface{}) runtime.Object {
	return assertActionFor(t, action, "update", "" /* subresource */, obj)
}

func assertUpdateStatus(t *testing.T, action clientgotesting.Action, obj interface{}) runtime.Object {
	return assertActionFor(t, action, "update", "status", obj)
}

func assertDelete(t *testing.T, action clientgotesting.Action, obj interface{}) {
	assertActionFor(t, action, "delete", "" /* subresource */, obj)
}

func assertActionFor(t *testing.T, action clientgotesting.Action, verb, subresource string, obj interface{}) runtime.Object {
	if e, a := verb, action.GetVerb(); e != a {
		fatalf(t, "Unexpected verb: expected %v, got %v\n\tactual action %q", e, a, action)
		return nil
	}

	var resource string

	switch obj.(type) {
	case *v1beta1.ClusterServiceBroker:
		resource = "clusterservicebrokers"
	case *v1beta1.ClusterServiceClass:
		resource = "clusterserviceclasses"
	case *v1beta1.ClusterServicePlan:
		resource = "clusterserviceplans"
	case *v1beta1.ServiceBroker:
		resource = "servicebrokers"
	case *v1beta1.ServiceClass:
		resource = "serviceclasses"
	case *v1beta1.ServicePlan:
		resource = "serviceplans"
	case *v1beta1.ServiceInstance:
		resource = "serviceinstances"
	case *v1beta1.ServiceBinding:
		resource = "servicebindings"
	}

	if e, a := resource, action.GetResource().Resource; e != a {
		fatalf(t, "Unexpected resource; expected %v, got %v", e, a)
		return nil
	}

	if e, a := subresource, action.GetSubresource(); e != a {
		fatalf(t, "Unexpected subresource; expected %v, got %v", e, a)
		return nil
	}

	rtObject, ok := obj.(runtime.Object)
	if !ok {
		fatalf(t, "Object %+v was not a runtime.Object", obj)
		return nil
	}

	paramAccessor, err := meta.Accessor(rtObject)
	if err != nil {
		fatalf(t, "Error creating ObjectMetaAccessor for param object %+v: %v", rtObject, err)
		return nil
	}

	var (
		objectMeta   metav1.Object
		fakeRtObject runtime.Object
	)

	switch verb {
	case "get":
		getAction, ok := action.(clientgotesting.GetAction)
		if !ok {
			fatalf(t, "Unexpected type; failed to convert action %+v to GetAction", action)
			return nil
		}

		if e, a := paramAccessor.GetName(), getAction.GetName(); e != a {
			fatalf(t, "Unexpected name: expected %v, got %v", e, a)
			return nil
		}

		return nil
	case "list":
		_, ok := action.(clientgotesting.ListAction)
		if !ok {
			fatalf(t, "Unexpected type; failed to convert action %+v to ListAction", action)
			return nil
		}
		return nil
	case "delete":
		deleteAction, ok := action.(clientgotesting.DeleteAction)
		if !ok {
			fatalf(t, "Unexpected type; failed to convert action %+v to DeleteAction", action)
			return nil
		}

		if e, a := paramAccessor.GetName(), deleteAction.GetName(); e != a {
			fatalf(t, "Unexpected name: expected %v, got %v", e, a)
			return nil
		}

		return nil
	case "create":
		createAction, ok := action.(clientgotesting.CreateAction)
		if !ok {
			fatalf(t, "Unexpected type; failed to convert action %+v to CreateAction", action)
			return nil
		}

		fakeRtObject = createAction.GetObject()
		objectMeta, err = meta.Accessor(fakeRtObject)
		if err != nil {
			fatalf(t, "Error creating ObjectMetaAccessor for %+v", fakeRtObject)
			return nil
		}
	case "update":
		updateAction, ok := action.(clientgotesting.UpdateAction)
		if !ok {
			fatalf(t, "Unexpected type; failed to convert action %+v to UpdateAction", action)
			return nil
		}

		fakeRtObject = updateAction.GetObject()
		objectMeta, err = meta.Accessor(fakeRtObject)
		if err != nil {
			fatalf(t, "Error creating ObjectMetaAccessor for %+v", fakeRtObject)
			return nil
		}
	}

	if e, a := paramAccessor.GetName(), objectMeta.GetName(); e != a {
		fatalf(t, "Unexpected name: expected %q, got %q", e, a)
		return nil
	}

	fakeValue := reflect.ValueOf(fakeRtObject)
	paramValue := reflect.ValueOf(obj)

	if e, a := paramValue.Type(), fakeValue.Type(); e != a {
		fatalf(t, "Unexpected type of object passed to fake client; expected %v, got %v", e, a)
		return nil
	}

	return fakeRtObject
}

func assertClassRemovedFromBrokerCatalogFalse(t *testing.T, obj runtime.Object) {
	assertClassRemovedFromBrokerCatalog(t, obj, false)
}

func assertClassRemovedFromBrokerCatalogTrue(t *testing.T, obj runtime.Object) {
	assertClassRemovedFromBrokerCatalog(t, obj, true)
}

func assertClassRemovedFromBrokerCatalog(t *testing.T, obj runtime.Object, condition bool) {
	clusterServiceClass, ok := obj.(*v1beta1.ClusterServiceClass)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ClusterServiceClass", obj)
	}

	if clusterServiceClass.Status.RemovedFromBrokerCatalog != condition {
		fatalf(t, "ClusterServiceClass.RemovedFromBrokerCatalog!=%v", condition)
	}
}

func assertPlanRemovedFromBrokerCatalogFalse(t *testing.T, obj runtime.Object) {
	assertPlanRemovedFromBrokerCatalog(t, obj, false)
}

func assertPlanRemovedFromBrokerCatalogTrue(t *testing.T, obj runtime.Object) {
	assertPlanRemovedFromBrokerCatalog(t, obj, true)
}

func assertPlanRemovedFromBrokerCatalog(t *testing.T, obj runtime.Object, condition bool) {
	clusterServicePlan, ok := obj.(*v1beta1.ClusterServicePlan)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ClusterServicePlan", obj)
	}

	if clusterServicePlan.Status.RemovedFromBrokerCatalog != condition {
		fatalf(t, "ClusterServicePlan.RemovedFromBrokerCatalog!=%v", condition)
	}
}

func assertClusterServiceBrokerReadyTrue(t *testing.T, obj runtime.Object) {
	assertClusterServiceBrokerCondition(t, obj, v1beta1.ServiceBrokerConditionReady, v1beta1.ConditionTrue)
}

func assertClusterServiceBrokerReadyFalse(t *testing.T, obj runtime.Object) {
	assertClusterServiceBrokerCondition(t, obj, v1beta1.ServiceBrokerConditionReady, v1beta1.ConditionFalse)
}

func assertClusterServiceBrokerCondition(t *testing.T, obj runtime.Object, conditionType v1beta1.ServiceBrokerConditionType, status v1beta1.ConditionStatus) {
	broker, ok := obj.(*v1beta1.ClusterServiceBroker)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ClusterServiceBroker", obj)
	}

	for _, condition := range broker.Status.Conditions {
		if condition.Type == conditionType && condition.Status != status {
			fatalf(t, "%v condition had unexpected status; expected %v, got %v", conditionType, status, condition.Status)
		}
	}
}

func assertClusterServiceBrokerOperationStartTimeSet(t *testing.T, obj runtime.Object, isOperationStartTimeSet bool) {
	broker, ok := obj.(*v1beta1.ClusterServiceBroker)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ClusterServiceBroker", obj)
	}

	if e, a := isOperationStartTimeSet, broker.Status.OperationStartTime != nil; e != a {
		if e {
			fatalf(t, "expected OperationStartTime to not be nil, but was")
		} else {
			fatalf(t, "expected OperationStartTime to be nil, but was not")
		}
	}
}

func assertServiceInstanceReadyTrue(t *testing.T, obj runtime.Object, reason ...string) {
	assertServiceInstanceReadyCondition(t, obj, v1beta1.ConditionTrue, reason...)
}

func assertServiceInstanceReadyFalse(t *testing.T, obj runtime.Object, reason ...string) {
	assertServiceInstanceReadyCondition(t, obj, v1beta1.ConditionFalse, reason...)
}

func assertServiceInstanceReadyCondition(t *testing.T, obj runtime.Object, status v1beta1.ConditionStatus, reason ...string) {
	assertServiceInstanceCondition(t, obj, v1beta1.ServiceInstanceConditionReady, status, reason...)
}

func assertServiceInstanceOrphanMitigationTrue(t *testing.T, obj runtime.Object, reason string) {
	assertServiceInstanceCondition(t, obj, v1beta1.ServiceInstanceConditionOrphanMitigation, v1beta1.ConditionTrue, reason)
}

func assertServiceInstanceCondition(t *testing.T, obj runtime.Object, conditionType v1beta1.ServiceInstanceConditionType, status v1beta1.ConditionStatus, reason ...string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	foundCondition := false
	for _, condition := range instance.Status.Conditions {
		if condition.Type == conditionType {
			foundCondition = true
			if condition.Status != status {
				fatalf(t, "%v condition had unexpected status; expected %v, got %v", conditionType, status, condition.Status)
			}
			if len(reason) == 1 && condition.Reason != reason[0] {
				fatalf(t, "unexpected reason; expected %v, got %v", reason[0], condition.Reason)
			}
		}
	}

	if !foundCondition {
		fatalf(t, "%v condition not found", conditionType)
	}
}

func assertServiceInstanceOrphanMitigationMissing(t *testing.T, obj runtime.Object, reason ...string) {
	assertServiceInstanceConditionMissing(t, obj, v1beta1.ServiceInstanceConditionOrphanMitigation)
}

func assertServiceInstanceConditionMissing(t *testing.T, obj runtime.Object, conditionType v1beta1.ServiceInstanceConditionType) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	for _, condition := range instance.Status.Conditions {
		if condition.Type == conditionType {
			fatalf(t, "%v condition expected to be missing, but was present with the reason %q and message %q", conditionType, condition.Reason, condition.Message)
			return
		}
	}
}

func assertServiceInstanceConditionsCount(t *testing.T, obj runtime.Object, count int) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := count, len(instance.Status.Conditions); e != a {
		fatalf(t, "Expected %v condition, got %v", e, a)
	}
}

func assertServiceInstanceCurrentOperationClear(t *testing.T, obj runtime.Object) {
	assertServiceInstanceCurrentOperation(t, obj, "")
}

func assertServiceInstanceCurrentOperation(t *testing.T, obj runtime.Object, currentOperation v1beta1.ServiceInstanceOperation) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := currentOperation, instance.Status.CurrentOperation; e != a {
		fatalf(t, "unexpected current operation: expected %q, got %q", e, a)
	}
}

func assertServiceInstanceReconciledGeneration(t *testing.T, obj runtime.Object, reconciledGeneration int64) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := reconciledGeneration, instance.Status.ReconciledGeneration; e != a {
		fatalf(t, "unexpected reconciled generation: expected %v, got %v", e, a)
	}
}

func assertServiceInstanceObservedGeneration(t *testing.T, obj runtime.Object, observedGeneration int64) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := observedGeneration, instance.Status.ObservedGeneration; e != a {
		fatalf(t, "unexpected observed generation: expected %v, got %v", e, a)
	}
}

func assertServiceInstanceProvisioned(t *testing.T, obj runtime.Object, provisionStatus v1beta1.ServiceInstanceProvisionStatus) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := provisionStatus, instance.Status.ProvisionStatus; e != a {
		fatalf(t, "unexpected provision status: expected %v, got %v", e, a)
	}
}

func assertServiceInstanceOperationStartTimeSet(t *testing.T, obj runtime.Object, isOperationStartTimeSet bool) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if e, a := isOperationStartTimeSet, instance.Status.OperationStartTime != nil; e != a {
		if e {
			fatalf(t, "expected OperationStartTime to not be nil, but was")
		} else {
			fatalf(t, "expected OperationStartTime to be nil, but was not")
		}
	}
}

func assertAsyncOpInProgressTrue(t *testing.T, obj runtime.Object) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if !instance.Status.AsyncOpInProgress {
		fatalf(t, "expected AsyncOpInProgress to be true but was %v", instance.Status.AsyncOpInProgress)
	}
}

func assertAsyncOpInProgressFalse(t *testing.T, obj runtime.Object) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if instance.Status.AsyncOpInProgress {
		fatalf(t, "expected AsyncOpInProgress to be false but was %v", instance.Status.AsyncOpInProgress)
	}
}

func assertServiceInstanceOrphanMitigationInProgressTrue(t *testing.T, obj runtime.Object) {
	assertServiceInstanceOrphanMitigationInProgress(t, obj, true)
}

func assertServiceInstanceOrphanMitigationInProgressFalse(t *testing.T, obj runtime.Object) {
	assertServiceInstanceOrphanMitigationInProgress(t, obj, false)
}

func assertServiceInstanceOrphanMitigationInProgress(t *testing.T, obj runtime.Object, expected bool) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	actual := isServiceInstanceOrphanMitigation(instance)
	if actual != expected {
		fatalf(t, "Expected OrphanMitigationInProgress to be %v but was %v", expected, actual)
	}
}

func assertServiceInstanceLastOperation(t *testing.T, obj runtime.Object, operation string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if instance.Status.LastOperation == nil {
		if operation != "" {
			fatalf(t, "Last Operation <nil> is not what was expected: %q", operation)
		}
	} else if *instance.Status.LastOperation != operation {
		fatalf(t, "Last Operation %q is not what was expected: %q", *instance.Status.LastOperation, operation)
	}
}

func assertServiceInstanceDashboardURL(t *testing.T, obj runtime.Object, dashboardURL string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if instance.Status.DashboardURL == nil {
		fatalf(t, "DashboardURL was nil")
	} else if *instance.Status.DashboardURL != dashboardURL {
		fatalf(t, "Unexpected DashboardURL: expected %q, got %q", dashboardURL, *instance.Status.DashboardURL)
	}
}

func assertServiceInstanceDeprovisionStatus(t *testing.T, obj runtime.Object, deprovisionStatus v1beta1.ServiceInstanceDeprovisionStatus) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if e, a := deprovisionStatus, instance.Status.DeprovisionStatus; e != a {
		fatalf(t, "Unexpected value for DeprovisionStatus: expected %v, got %v", e, a)
	}
}

func assertServiceInstanceErrorBeforeRequest(t *testing.T, obj runtime.Object, reason string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceReadyFalse(t, obj, reason)
	assertServiceInstanceCurrentOperationClear(t, obj)
	assertServiceInstanceOperationStartTimeSet(t, obj, false)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertAsyncOpInProgressFalse(t, obj)
	assertServiceInstanceOrphanMitigationInProgressFalse(t, obj)
	assertServiceInstanceInProgressPropertiesNil(t, obj)
	assertServiceInstanceExternalPropertiesUnchanged(t, obj, originalInstance)
	assertServiceInstanceDeprovisionStatus(t, obj, originalInstance.Status.DeprovisionStatus)
}

func assertServiceInstanceOperationInProgress(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, planName, planID string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceOperationInProgressWithParameters(t, obj, operation, planName, planID, nil, "", originalInstance)
}

func assertServiceInstanceOperationInProgressWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, planName, planID string, inProgressParameters map[string]interface{}, inProgressParametersChecksum string, originalInstance *v1beta1.ServiceInstance) {
	reason := ""
	var expectedObservedGeneration int64
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		reason = provisioningInFlightReason
		expectedObservedGeneration = originalInstance.Generation
	case v1beta1.ServiceInstanceOperationUpdate:
		reason = instanceUpdatingInFlightReason
		expectedObservedGeneration = originalInstance.Generation
	case v1beta1.ServiceInstanceOperationDeprovision:
		reason = deprovisioningInFlightReason
		if isServiceInstanceOrphanMitigation(originalInstance) {
			expectedObservedGeneration = originalInstance.Status.ObservedGeneration
		} else {
			expectedObservedGeneration = originalInstance.Generation
		}
	}
	assertServiceInstanceReadyFalse(t, obj, reason)
	assertServiceInstanceCurrentOperation(t, obj, operation)
	assertServiceInstanceOperationStartTimeSet(t, obj, true)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, expectedObservedGeneration)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertAsyncOpInProgressFalse(t, obj)
	assertServiceInstanceOrphanMitigationInProgressFalse(t, obj)
	assertServiceInstanceInProgressPropertiesPlan(t, obj, planName, planID)
	assertServiceInstanceInProgressPropertiesParameters(t, obj, inProgressParameters, inProgressParametersChecksum)
	assertServiceInstanceExternalPropertiesUnchanged(t, obj, originalInstance)
	assertServiceInstanceDeprovisionStatus(t, obj, v1beta1.ServiceInstanceDeprovisionStatusRequired)
}

func assertServiceInstanceStartingOrphanMitigation(t *testing.T, obj runtime.Object, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceCurrentOperation(t, obj, v1beta1.ServiceInstanceOperationProvision)
	assertServiceInstanceReadyFalse(t, obj, startingInstanceOrphanMitigationReason)
	assertServiceInstanceOperationStartTimeSet(t, obj, true)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertServiceInstanceOrphanMitigationTrue(t, obj, errorProvisionCallFailedReason)
	assertServiceInstanceOrphanMitigationInProgressTrue(t, obj)
	assertServiceInstanceDeprovisionStatus(t, obj, v1beta1.ServiceInstanceDeprovisionStatusRequired)
}

func assertServiceInstanceOperationSuccess(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, planName, planID string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceOperationSuccessWithParameters(t, obj, operation, planName, planID, nil, "", originalInstance)
}

func assertServiceInstanceOperationSuccessWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, planName, planID string, externalParameters map[string]interface{}, externalParametersChecksum string, originalInstance *v1beta1.ServiceInstance) {
	var (
		reason            string
		readyStatus       v1beta1.ConditionStatus
		deprovisionStatus v1beta1.ServiceInstanceDeprovisionStatus
	)
	var provisionStatus v1beta1.ServiceInstanceProvisionStatus
	var observedGeneration int64
	var reconciledGeneration int64
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		provisionStatus = v1beta1.ServiceInstanceProvisionStatusProvisioned
		reason = successProvisionReason
		readyStatus = v1beta1.ConditionTrue
		deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusRequired
		observedGeneration = originalInstance.Generation
		reconciledGeneration = observedGeneration
	case v1beta1.ServiceInstanceOperationUpdate:
		provisionStatus = v1beta1.ServiceInstanceProvisionStatusProvisioned
		reason = successUpdateInstanceReason
		readyStatus = v1beta1.ConditionTrue
		deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusRequired
		observedGeneration = originalInstance.Generation
		reconciledGeneration = observedGeneration
	case v1beta1.ServiceInstanceOperationDeprovision:
		provisionStatus = v1beta1.ServiceInstanceProvisionStatusNotProvisioned
		reason = successDeprovisionReason
		readyStatus = v1beta1.ConditionFalse
		deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusSucceeded
		if isServiceInstanceOrphanMitigation(originalInstance) {
			observedGeneration = originalInstance.Status.ObservedGeneration
		} else {
			observedGeneration = originalInstance.Generation
		}
		reconciledGeneration = originalInstance.Status.ReconciledGeneration
	}
	assertServiceInstanceReadyCondition(t, obj, readyStatus, reason)
	assertServiceInstanceCurrentOperationClear(t, obj)
	assertServiceInstanceOperationStartTimeSet(t, obj, false)
	assertServiceInstanceReconciledGeneration(t, obj, reconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, observedGeneration)
	assertServiceInstanceProvisioned(t, obj, provisionStatus)
	assertAsyncOpInProgressFalse(t, obj)
	assertServiceInstanceOrphanMitigationInProgressFalse(t, obj)
	if operation == v1beta1.ServiceInstanceOperationDeprovision {
		assertEmptyFinalizers(t, obj)
	}
	assertServiceInstanceInProgressPropertiesNil(t, obj)
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision, v1beta1.ServiceInstanceOperationUpdate:
		assertServiceInstanceExternalPropertiesPlan(t, obj, planName, planID)
		assertServiceInstanceExternalPropertiesParameters(t, obj, externalParameters, externalParametersChecksum)
	case v1beta1.ServiceInstanceOperationDeprovision:
		assertServiceInstanceExternalPropertiesNil(t, obj)
	}
	assertServiceInstanceDeprovisionStatus(t, obj, deprovisionStatus)
}

func assertServiceInstanceRequestFailingError(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, readyReason string, failureReason string, orphanMitigationRequired bool, originalInstance *v1beta1.ServiceInstance) {
	var (
		readyStatus       v1beta1.ConditionStatus
		deprovisionStatus v1beta1.ServiceInstanceDeprovisionStatus
	)
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		readyStatus = v1beta1.ConditionFalse
		if orphanMitigationRequired {
			deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusRequired
		} else {
			deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusNotRequired
		}
	case v1beta1.ServiceInstanceOperationUpdate:
		readyStatus = v1beta1.ConditionFalse
		deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusRequired
	case v1beta1.ServiceInstanceOperationDeprovision:
		readyStatus = v1beta1.ConditionUnknown
		deprovisionStatus = v1beta1.ServiceInstanceDeprovisionStatusFailed
	}

	assertServiceInstanceReadyCondition(t, obj, readyStatus, readyReason)

	if failureReason == "" {
		assertServiceInstanceConditionMissing(t, obj, v1beta1.ServiceInstanceConditionFailed)
	} else {
		assertServiceInstanceCondition(t, obj, v1beta1.ServiceInstanceConditionFailed, v1beta1.ConditionTrue, failureReason)
	}

	if failureReason == "" || orphanMitigationRequired {
		assertServiceInstanceOperationStartTimeSet(t, obj, true)
	} else {
		assertServiceInstanceOperationStartTimeSet(t, obj, false)
	}

	assertAsyncOpInProgressFalse(t, obj)
	assertServiceInstanceExternalPropertiesUnchanged(t, obj, originalInstance)
	assertServiceInstanceDeprovisionStatus(t, obj, deprovisionStatus)
}

func assertServiceInstanceProvisionRequestFailingErrorNoOrphanMitigation(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, readyReason string, failureReason string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceRequestFailingError(t, obj, operation, readyReason, failureReason, false, originalInstance)

	if failureReason == "" {
		assertServiceInstanceCurrentOperation(t, obj, operation)
		assertServiceInstanceInProgressPropertiesUnchanged(t, obj, originalInstance)
	} else {
		assertServiceInstanceCurrentOperationClear(t, obj)
		assertServiceInstanceInProgressPropertiesNil(t, obj)
	}

	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertServiceInstanceOrphanMitigationInProgressFalse(t, obj)

}

func assertServiceInstanceUpdateRequestFailingErrorNoOrphanMitigation(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, readyReason string, failureReason string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceRequestFailingError(t, obj, operation, readyReason, failureReason, false, originalInstance)

	if failureReason == "" {
		assertServiceInstanceCurrentOperation(t, obj, operation)
		assertServiceInstanceInProgressPropertiesUnchanged(t, obj, originalInstance)
	} else {
		assertServiceInstanceCurrentOperationClear(t, obj)
		assertServiceInstanceInProgressPropertiesNil(t, obj)
	}

	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertServiceInstanceOrphanMitigationInProgressFalse(t, obj)
}

func assertServiceInstanceRequestFailingErrorStartOrphanMitigation(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, readyReason string, failureReason string, orphanMitigationReason string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceRequestFailingError(t, obj, operation, readyReason, failureReason, true, originalInstance)
	assertServiceInstanceCurrentOperation(t, obj, v1beta1.ServiceInstanceOperationProvision)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertServiceInstanceOrphanMitigationTrue(t, obj, orphanMitigationReason)
	assertServiceInstanceOrphanMitigationInProgressTrue(t, obj)
}

func assertServiceInstanceRequestRetriableError(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, reason string, planName, planID string, originalInstance *v1beta1.ServiceInstance) {
	assertServiceInstanceRequestRetriableErrorWithParameters(t, obj, operation, reason, planName, planID, nil, "", originalInstance)
}

func assertServiceInstanceRequestRetriableErrorWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, reason string, planName, planID string, inProgressParameters map[string]interface{}, inProgressParametersChecksum string, originalInstance *v1beta1.ServiceInstance) {
	var readyStatus v1beta1.ConditionStatus
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision, v1beta1.ServiceInstanceOperationUpdate:
		readyStatus = v1beta1.ConditionFalse
	case v1beta1.ServiceInstanceOperationDeprovision:
		readyStatus = v1beta1.ConditionUnknown
	}
	assertServiceInstanceReadyCondition(t, obj, readyStatus, reason)
	assertServiceInstanceCurrentOperation(t, obj, operation)
	assertServiceInstanceOperationStartTimeSet(t, obj, true)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision, v1beta1.ServiceInstanceOperationUpdate:
	case v1beta1.ServiceInstanceOperationDeprovision:
		assertServiceInstanceInProgressPropertiesPlan(t, obj, planName, planID)
		assertServiceInstanceInProgressPropertiesParameters(t, obj, inProgressParameters, inProgressParametersChecksum)
	}
	assertServiceInstanceExternalPropertiesUnchanged(t, obj, originalInstance)
	assertServiceInstanceDeprovisionStatus(t, obj, originalInstance.Status.DeprovisionStatus)
}

func assertServiceInstanceAsyncStartInProgress(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, operationKey string, planName, planID string, originalInstance *v1beta1.ServiceInstance) {
	reason := ""
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		reason = asyncProvisioningReason
	case v1beta1.ServiceInstanceOperationUpdate:
		reason = asyncUpdatingInstanceReason
	case v1beta1.ServiceInstanceOperationDeprovision:
		reason = asyncDeprovisioningReason
	}
	assertServiceInstanceReadyFalse(t, obj, reason)
	assertServiceInstanceLastOperation(t, obj, operationKey)
	assertServiceInstanceCurrentOperation(t, obj, operation)
	assertServiceInstanceOperationStartTimeSet(t, obj, true)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Generation)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	assertServiceInstanceInProgressPropertiesPlan(t, obj, planName, planID)
	assertServiceInstanceInProgressPropertiesParameters(t, obj, nil, "")
	assertAsyncOpInProgressTrue(t, obj)
	assertServiceInstanceDeprovisionStatus(t, obj, originalInstance.Status.DeprovisionStatus)
}

func assertServiceInstanceAsyncStillInProgress(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, operationKey string, planName, planID string, originalInstance *v1beta1.ServiceInstance) {
	reason := ""
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		reason = asyncProvisioningReason
	case v1beta1.ServiceInstanceOperationUpdate:
		reason = asyncUpdatingInstanceReason
	case v1beta1.ServiceInstanceOperationDeprovision:
		reason = asyncDeprovisioningReason
	}
	assertServiceInstanceReadyFalse(t, obj, reason)
	assertServiceInstanceLastOperation(t, obj, operationKey)
	assertServiceInstanceCurrentOperation(t, obj, operation)
	assertServiceInstanceOperationStartTimeSet(t, obj, true)
	assertServiceInstanceReconciledGeneration(t, obj, originalInstance.Status.ReconciledGeneration)
	assertServiceInstanceObservedGeneration(t, obj, originalInstance.Status.ObservedGeneration)
	assertServiceInstanceProvisioned(t, obj, originalInstance.Status.ProvisionStatus)
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision, v1beta1.ServiceInstanceOperationUpdate:
	case v1beta1.ServiceInstanceOperationDeprovision:
		assertServiceInstanceInProgressPropertiesPlan(t, obj, planName, planID)
		assertServiceInstanceInProgressPropertiesParameters(t, obj, nil, "")
	}
	assertAsyncOpInProgressTrue(t, obj)
	assertServiceInstanceDeprovisionStatus(t, obj, originalInstance.Status.DeprovisionStatus)
}

func assertServiceInstanceConditionHasLastOperationDescription(t *testing.T, obj runtime.Object, operation v1beta1.ServiceInstanceOperation, lastOperationDescription string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	var expected string
	switch operation {
	case v1beta1.ServiceInstanceOperationProvision:
		expected = fmt.Sprintf("%s (%s)", asyncProvisioningMessage, lastOperationDescription)
	case v1beta1.ServiceInstanceOperationUpdate:
		expected = fmt.Sprintf("%s (%s)", asyncUpdatingInstanceMessage, lastOperationDescription)
	case v1beta1.ServiceInstanceOperationDeprovision:
		expected = fmt.Sprintf("%s (%s)", asyncDeprovisioningMessage, lastOperationDescription)
	}
	if e, a := expected, instance.Status.Conditions[0].Message; e != a {
		fatalf(t, "unexpected condition message: expected %q, got %q", e, a)
	}
}

func assertServiceInstanceInProgressPropertiesNil(t *testing.T, obj runtime.Object) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if a := instance.Status.InProgressProperties; a != nil {
		fatalf(t, "expected in-progress properties to be nil: actual %v", a)
	}
}

func assertServiceInstanceInProgressPropertiesPlan(t *testing.T, obj runtime.Object, planName string, planID string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	assertServiceInstancePropertiesStatePlan(t, "in-progress", instance, instance.Status.InProgressProperties, planName, planID)
}

func assertServiceInstanceInProgressPropertiesParameters(t *testing.T, obj runtime.Object, params map[string]interface{}, checksum string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	assertServiceInstancePropertiesStateParameters(t, "in-progress", instance.Status.InProgressProperties, params, checksum)
}

func assertServiceInstanceInProgressPropertiesUnchanged(t *testing.T, obj runtime.Object, originalInstance *v1beta1.ServiceInstance) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if originalInstance.Status.InProgressProperties == nil {
		assertServiceInstanceInProgressPropertiesNil(t, obj)
	} else {
		assertServiceInstancePropertiesStateParametersUnchanged(t, "in-progress", instance.Status.InProgressProperties, *originalInstance.Status.InProgressProperties)
	}
}

func assertServiceInstanceExternalPropertiesNil(t *testing.T, obj runtime.Object) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}

	if a := instance.Status.ExternalProperties; a != nil {
		fatalf(t, "expected external properties to be nil: actual %v", a)
	}
}

func assertServiceInstanceExternalPropertiesPlan(t *testing.T, obj runtime.Object, planName string, planID string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	assertServiceInstancePropertiesStatePlan(t, "external", instance, instance.Status.ExternalProperties, planName, planID)
}

func assertServiceInstanceExternalPropertiesParameters(t *testing.T, obj runtime.Object, params map[string]interface{}, checksum string) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	assertServiceInstancePropertiesStateParameters(t, "external", instance.Status.ExternalProperties, params, checksum)
}

func assertServiceInstanceExternalPropertiesUnchanged(t *testing.T, obj runtime.Object, originalInstance *v1beta1.ServiceInstance) {
	instance, ok := obj.(*v1beta1.ServiceInstance)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceInstance", obj)
	}
	if originalInstance.Status.ExternalProperties == nil {
		assertServiceInstanceExternalPropertiesNil(t, obj)
	} else {
		assertServiceInstancePropertiesStateParametersUnchanged(t, "external", instance.Status.ExternalProperties, *originalInstance.Status.ExternalProperties)
	}
}

func assertServiceInstancePropertiesStatePlan(t *testing.T, propsLabel string, instance *v1beta1.ServiceInstance, actualProps *v1beta1.ServiceInstancePropertiesState, expectedPlanName string, expectedPlanID string) {
	if actualProps == nil {
		fatalf(t, "expected %v properties to not be nil", propsLabel)
	}

	if instance.Spec.ClusterServicePlanSpecified() {
		if e, a := expectedPlanName, actualProps.ClusterServicePlanExternalName; e != a {
			fatalf(t, "unexpected %v properties external service plan name: expected %v, actual %v", propsLabel, e, a)
		}
		if e, a := expectedPlanID, actualProps.ClusterServicePlanExternalID; e != a {
			fatalf(t, "unexpected %v properties external service plan ID: expected %v, actual %v", propsLabel, e, a)
		}
	} else {
		if e, a := expectedPlanName, actualProps.ServicePlanExternalName; e != a {
			fatalf(t, "unexpected %v properties external service plan name: expected %v, actual %v", propsLabel, e, a)
		}
		if e, a := expectedPlanID, actualProps.ServicePlanExternalID; e != a {
			fatalf(t, "unexpected %v properties external service plan ID: expected %v, actual %v", propsLabel, e, a)
		}
	}

}

func assertServiceInstancePropertiesStateParameters(t *testing.T, propsLabel string, actualProps *v1beta1.ServiceInstancePropertiesState, expectedParams map[string]interface{}, expectedChecksum string) {
	if actualProps == nil {
		fatalf(t, "expected %v properties to not be nil", propsLabel)
	}
	assertPropertiesStateParameters(t, propsLabel, actualProps.Parameters, expectedParams)
	if e, a := expectedChecksum, actualProps.ParameterChecksum; e != a {
		fatalf(t, "unexpected %v properties parameters checksum: expected %v, actual %v", propsLabel, e, a)
	}
}

func assertPropertiesStateParameters(t *testing.T, propsLabel string, marshalledParams *runtime.RawExtension, expectedParams map[string]interface{}) {
	if expectedParams == nil {
		if a := marshalledParams; a != nil {
			fatalf(t, "expected %v properties parameters to be nil: actual %v", propsLabel, a)
		}
	} else {
		if marshalledParams == nil {
			fatalf(t, "expected %v properties parameters to not be nil", propsLabel)
		}
		actualParams := make(map[string]interface{})
		if err := yaml.Unmarshal(marshalledParams.Raw, &actualParams); err != nil {
			fatalf(t, "%v properties parameters could not be unmarshalled: %v", propsLabel, err)
		}
		if e, a := expectedParams, actualParams; !reflect.DeepEqual(e, a) {
			fatalf(t, "unexpected %v properties parameters: expected %v, actual %v", propsLabel, e, a)
		}
	}
}

func assertServiceInstancePropertiesStateParametersUnchanged(t *testing.T, propsLabel string, new *v1beta1.ServiceInstancePropertiesState, old v1beta1.ServiceInstancePropertiesState) {
	if new == nil {
		fatalf(t, "expected %v properties to not be nil", propsLabel)
	}
	if e, a := old, *new; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected %v properties: expected %v, actual %v", propsLabel, e, a)
	}
}

func assertServiceBindingReadyTrue(t *testing.T, obj runtime.Object) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionTrue)
}

func assertServiceBindingReadyFalse(t *testing.T, obj runtime.Object, reason ...string) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionFalse, reason...)
}

func assertServiceBindingReadyCondition(t *testing.T, obj runtime.Object, status v1beta1.ConditionStatus, reason ...string) {
	assertServiceBindingCondition(t, obj, v1beta1.ServiceBindingConditionReady, status, reason...)
}

func assertServiceBindingCondition(t *testing.T, obj runtime.Object, conditionType v1beta1.ServiceBindingConditionType, status v1beta1.ConditionStatus, reason ...string) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	conditionFound := false
	for _, condition := range binding.Status.Conditions {
		if condition.Type == conditionType {
			conditionFound = true
			if condition.Status != status {
				t.Logf("%v condition: %+v", conditionType, condition)
				fatalf(t, "%v condition had unexpected status; expected %v, got %v", conditionType, status, condition.Status)
			}
			if len(reason) == 1 && condition.Reason != reason[0] {
				fatalf(t, "unexpected reason; expected %v, got %v", reason[0], condition.Reason)
			}
		}
	}

	if !conditionFound {
		fatalf(t, "unfound %v condition", conditionType)
	}
}

func assertServiceBindingReconciledGeneration(t *testing.T, obj runtime.Object, reconciledGeneration int64) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if e, a := reconciledGeneration, binding.Status.ReconciledGeneration; e != a {
		fatalf(t, "unexpected reconciled generation: expected %v, got %v", e, a)
	}
}

func assertServiceBindingReconciliationNotComplete(t *testing.T, obj runtime.Object) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if g, rg := binding.Generation, binding.Status.ReconciledGeneration; g <= rg {
		fatalf(t, "expected ReconciledGeneration to be less than Generation: Generation %v, ReconciledGeneration %v", g, rg)
	}
}

func assertServiceBindingOperationStartTimeSet(t *testing.T, obj runtime.Object, isOperationStartTimeSet bool) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if e, a := isOperationStartTimeSet, binding.Status.OperationStartTime != nil; e != a {
		if e {
			fatalf(t, "expected OperationStartTime to not be nil, but was")
		} else {
			fatalf(t, "expected OperationStartTime to be nil, but was not")
		}
	}
}

func assertServiceBindingCurrentOperationClear(t *testing.T, obj runtime.Object) {
	assertServiceBindingCurrentOperation(t, obj, "")
}

func assertServiceBindingCurrentOperation(t *testing.T, obj runtime.Object, currentOperation v1beta1.ServiceBindingOperation) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if e, a := currentOperation, binding.Status.CurrentOperation; e != a {
		fatalf(t, "unexpected current operation: expected %q, got %q", e, a)
	}
}

func assertServiceBindingErrorBeforeRequest(t *testing.T, obj runtime.Object, reason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyFalse(t, obj, reason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusNotRequired)
}

func assertServiceBindingFailedBeforeRequest(t *testing.T, obj runtime.Object, reason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyFalse(t, obj, reason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusNotRequired)
}

func assertServiceBindingOperationInProgress(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingOperationInProgressWithParameters(t, obj, operation, nil, "", originalBinding)
}

func assertServiceBindingOperationInProgressWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, inProgressParameters map[string]interface{}, inProgressParametersChecksum string, originalBinding *v1beta1.ServiceBinding) {
	reason := ""
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		reason = bindingInFlightReason
	case v1beta1.ServiceBindingOperationUnbind:
		reason = unbindingInFlightReason
	}
	assertServiceBindingReadyFalse(t, obj, reason)
	assertServiceBindingCurrentOperation(t, obj, operation)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		assertServiceBindingInProgressPropertiesParameters(t, obj, inProgressParameters, inProgressParametersChecksum)
	case v1beta1.ServiceBindingOperationUnbind:
		assertServiceBindingInProgressPropertiesNil(t, obj)
	}
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
}

func assertServiceBindingStartingOrphanMitigation(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingReadyFalse(t, obj, errorServiceBindingOrphanMitigation)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingOrphanMitigationSet(t, obj, true)
	assertServiceBindingInProgressPropertiesParameters(t, obj, nil, "")
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
}

func assertServiceBindingOperationSuccess(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingOperationSuccessWithParameters(t, obj, operation, nil, "", originalBinding)
}

func assertServiceBindingOperationSuccessWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, externalParameters map[string]interface{}, externalParametersChecksum string, originalBinding *v1beta1.ServiceBinding) {
	var (
		reason       string
		readyStatus  v1beta1.ConditionStatus
		unbindStatus v1beta1.ServiceBindingUnbindStatus
	)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		reason = successInjectedBindResultReason
		readyStatus = v1beta1.ConditionTrue
		unbindStatus = v1beta1.ServiceBindingUnbindStatusRequired
	case v1beta1.ServiceBindingOperationUnbind:
		reason = successUnboundReason
		readyStatus = v1beta1.ConditionFalse
		unbindStatus = v1beta1.ServiceBindingUnbindStatusSucceeded
	}
	assertServiceBindingReadyCondition(t, obj, readyStatus, reason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	if operation == v1beta1.ServiceBindingOperationUnbind {
		assertEmptyFinalizers(t, obj)
	} else {
		assertCatalogFinalizerExists(t, obj)
	}
	assertServiceBindingInProgressPropertiesNil(t, obj)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		assertServiceBindingExternalPropertiesParameters(t, obj, externalParameters, externalParametersChecksum)
	case v1beta1.ServiceBindingOperationUnbind:
		assertServiceBindingExternalPropertiesNil(t, obj)
	}
	assertServiceBindingUnbindStatus(t, obj, unbindStatus)
}

func assertServiceBindingRequestFailingError(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, readyReason string, failureReason string, originalBinding *v1beta1.ServiceBinding) {
	var (
		readyStatus  v1beta1.ConditionStatus
		unbindStatus v1beta1.ServiceBindingUnbindStatus
	)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		readyStatus = v1beta1.ConditionFalse
		unbindStatus = v1beta1.ServiceBindingUnbindStatusRequired
	case v1beta1.ServiceBindingOperationUnbind:
		readyStatus = v1beta1.ConditionUnknown
		unbindStatus = v1beta1.ServiceBindingUnbindStatusFailed
	}
	assertServiceBindingReadyCondition(t, obj, readyStatus, readyReason)
	assertServiceBindingCondition(t, obj, v1beta1.ServiceBindingConditionFailed, v1beta1.ConditionTrue, failureReason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, unbindStatus)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingAsyncBindRetryDurationExceeded(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionFalse, errorServiceBindingOrphanMitigation)
	assertServiceBindingCondition(t, obj, v1beta1.ServiceBindingConditionFailed, v1beta1.ConditionTrue, errorReconciliationRetryTimeoutReason)
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingInProgressPropertiesParameters(t, obj, nil, "")
	assertServiceBindingExternalPropertiesNil(t, obj)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingAsyncBindErrorAfterStateSucceeded(t *testing.T, obj runtime.Object, failureReason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionFalse, errorServiceBindingOrphanMitigation)
	assertServiceBindingCondition(t, obj, v1beta1.ServiceBindingConditionFailed, v1beta1.ConditionTrue, failureReason)
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingExternalPropertiesParameters(t, obj, nil, "")
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
	assertServiceBindingInProgressPropertiesParameters(t, obj, nil, "")
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingAsyncUnbindRetryDurationExceeded(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, readyReason string, failureReason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionUnknown, readyReason)
	assertServiceBindingCondition(t, obj, v1beta1.ServiceBindingConditionFailed, v1beta1.ConditionTrue, failureReason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusFailed)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingAsyncOrphanMitigationRetryDurationExceeded(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionUnknown, errorOrphanMitigationFailedReason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesNil(t, obj)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusFailed)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingErrorFetchingBinding(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionFalse, errorFetchingBindingFailedReason)
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesParameters(t, obj, nil, "")
	assertServiceBindingAsyncOpInProgressTrue(t, obj)
	assertServiceBindingOrphanMitigationSet(t, obj, false)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingErrorInjectingCredentials(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyFalse(t, obj, errorInjectingBindResultReason)
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	// External properties are updated because the bind request with the Broker was successful
	assertServiceBindingExternalPropertiesParameters(t, obj, nil, "")
	assertServiceBindingAsyncOpInProgressTrue(t, obj)
	assertServiceBindingOrphanMitigationSet(t, obj, false)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingRequestRetriableError(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, reason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingRequestRetriableErrorWithParameters(t, obj, operation, reason, nil, "", originalBinding)
}

func assertServiceBindingRequestRetriableErrorWithParameters(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, reason string, inProgressParameters map[string]interface{}, inProgressParametersChecksum string, originalBinding *v1beta1.ServiceBinding) {
	var readyStatus v1beta1.ConditionStatus
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		readyStatus = v1beta1.ConditionFalse
	case v1beta1.ServiceBindingOperationUnbind:
		readyStatus = v1beta1.ConditionUnknown
	}
	assertServiceBindingReadyCondition(t, obj, readyStatus, reason)
	assertServiceBindingCurrentOperation(t, obj, operation)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		assertServiceBindingInProgressPropertiesParameters(t, obj, inProgressParameters, inProgressParametersChecksum)
	case v1beta1.ServiceBindingOperationUnbind:
		assertServiceBindingInProgressPropertiesNil(t, obj)
	}
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
}

func assertServiceBindingRequestRetriableOrphanMitigation(t *testing.T, obj runtime.Object, reason string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionUnknown, reason)
	assertServiceBindingCurrentOperation(t, obj, v1beta1.ServiceBindingOperationBind)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	assertServiceBindingInProgressPropertiesParameters(t, obj, nil, "")
	assertServiceBindingExternalPropertiesUnchanged(t, obj, originalBinding)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
}

func assertServiceBindingAsyncInProgress(t *testing.T, obj runtime.Object, operation v1beta1.ServiceBindingOperation, reason string, operationKey string, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyFalse(t, obj, reason)
	assertServiceBindingLastOperation(t, obj, operationKey)
	assertServiceBindingCurrentOperation(t, obj, operation)
	assertServiceBindingOperationStartTimeSet(t, obj, true)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Status.ReconciledGeneration)
	switch operation {
	case v1beta1.ServiceBindingOperationBind:
		assertServiceBindingInProgressPropertiesParameters(t, obj, nil, "")
	case v1beta1.ServiceBindingOperationUnbind:
		assertServiceBindingInProgressPropertiesNil(t, obj)
	}
	assertServiceBindingAsyncOpInProgressTrue(t, obj)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusRequired)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingOrphanMitigationSuccess(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionFalse, successOrphanMitigationReason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesNil(t, obj)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusSucceeded)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingOrphanMitigationFailure(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	assertServiceBindingReadyCondition(t, obj, v1beta1.ConditionUnknown, errorOrphanMitigationFailedReason)
	assertServiceBindingCurrentOperationClear(t, obj)
	assertServiceBindingOperationStartTimeSet(t, obj, false)
	assertServiceBindingReconciledGeneration(t, obj, originalBinding.Generation)
	assertServiceBindingInProgressPropertiesNil(t, obj)
	assertServiceBindingExternalPropertiesNil(t, obj)
	assertServiceBindingUnbindStatus(t, obj, v1beta1.ServiceBindingUnbindStatusFailed)
	assertCatalogFinalizerExists(t, obj)
}

func assertServiceBindingLastOperation(t *testing.T, obj runtime.Object, operation string) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if binding.Status.LastOperation == nil {
		if operation != "" {
			fatalf(t, "Last Operation <nil> is not what was expected: %q", operation)
		}
	} else if *binding.Status.LastOperation != operation {
		fatalf(t, "Last Operation %q is not what was expected: %q", *binding.Status.LastOperation, operation)
	}
}

func assertServiceBindingAsyncOpInProgressTrue(t *testing.T, obj runtime.Object) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if !binding.Status.AsyncOpInProgress {
		fatalf(t, "expected AsyncOpInProgress to be true but was %v", binding.Status.AsyncOpInProgress)
	}
}

func assertServiceBindingAsyncOpInProgressFalse(t *testing.T, obj runtime.Object) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if binding.Status.AsyncOpInProgress {
		fatalf(t, "expected AsyncOpInProgress to be false but was %v", binding.Status.AsyncOpInProgress)
	}
}

func assertServiceBindingInProgressPropertiesNil(t *testing.T, obj runtime.Object) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if a := binding.Status.InProgressProperties; a != nil {
		fatalf(t, "expected in-progress properties to be nil: actual %v", a)
	}
}

func assertServiceBindingInProgressPropertiesParameters(t *testing.T, obj runtime.Object, params map[string]interface{}, checksum string) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	assertServiceBindingPropertiesStateParameters(t, "in-progress", binding.Status.InProgressProperties, params, checksum)
}

func assertServiceBindingInProgressPropertiesUnchanged(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if originalBinding.Status.InProgressProperties == nil {
		assertServiceBindingInProgressPropertiesNil(t, obj)
	} else {
		assertServiceBindingPropertiesStateParametersUnchanged(t, "in-progress", binding.Status.InProgressProperties, *originalBinding.Status.InProgressProperties)
	}
}

func assertServiceBindingExternalPropertiesNil(t *testing.T, obj runtime.Object) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if a := binding.Status.ExternalProperties; a != nil {
		fatalf(t, "expected external properties to be nil: actual %v", a)
	}
}

func assertServiceBindingExternalPropertiesParameters(t *testing.T, obj runtime.Object, params map[string]interface{}, checksum string) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	assertServiceBindingPropertiesStateParameters(t, "external", binding.Status.ExternalProperties, params, checksum)
}

func assertServiceBindingExternalPropertiesUnchanged(t *testing.T, obj runtime.Object, originalBinding *v1beta1.ServiceBinding) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}
	if originalBinding.Status.ExternalProperties == nil {
		assertServiceBindingExternalPropertiesNil(t, obj)
	} else {
		assertServiceBindingPropertiesStateParametersUnchanged(t, "external", binding.Status.ExternalProperties, *originalBinding.Status.ExternalProperties)
	}
}

func assertServiceBindingPropertiesStateParameters(t *testing.T, propsLabel string, actualProps *v1beta1.ServiceBindingPropertiesState, expectedParams map[string]interface{}, expectedChecksum string) {
	if actualProps == nil {
		fatalf(t, "expected %v properties to not be nil", propsLabel)
	}
	assertPropertiesStateParameters(t, propsLabel, actualProps.Parameters, expectedParams)
	if e, a := expectedChecksum, actualProps.ParameterChecksum; e != a {
		fatalf(t, "unexpected %v properties parameters checksum: expected %v, actual %v", propsLabel, e, a)
	}
}

func assertServiceBindingPropertiesStateParametersUnchanged(t *testing.T, propsLabel string, new *v1beta1.ServiceBindingPropertiesState, old v1beta1.ServiceBindingPropertiesState) {
	if new == nil {
		fatalf(t, "expected %v properties to not be nil", propsLabel)
	}
	if e, a := old, *new; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected %v properties: expected %v, actual %v", propsLabel, e, a)
	}
}

func assertServiceBindingOrphanMitigationSet(t *testing.T, obj runtime.Object, inProgress bool) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if e, a := inProgress, binding.Status.OrphanMitigationInProgress; e != a {
		fatalf(t, "expected OrphanMitigationInProgress to be %v, but was not", a)
	}
}

func assertServiceBindingUnbindStatus(t *testing.T, obj runtime.Object, unbindStatus v1beta1.ServiceBindingUnbindStatus) {
	binding, ok := obj.(*v1beta1.ServiceBinding)
	if !ok {
		fatalf(t, "Couldn't convert object %+v into a *v1beta1.ServiceBinding", obj)
	}

	if e, a := unbindStatus, binding.Status.UnbindStatus; e != a {
		fatalf(t, "unexpected UnbindStatus, %s", expectedGot(e, a))
	}
}

func assertEmptyFinalizers(t *testing.T, obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		fatalf(t, "Error creating ObjectMetaAccessor for param object %+v: %v", obj, err)
	}

	if len(accessor.GetFinalizers()) != 0 {
		fatalf(t, "Unexpected number of finalizers; expected 0, got %v", len(accessor.GetFinalizers()))
	}
}

func assertCatalogFinalizerExists(t *testing.T, obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		fatalf(t, "Error creating ObjectMetaAccessor for param object %+v: %v", obj, err)
	}

	finalizers := sets.NewString(accessor.GetFinalizers()...)
	if !finalizers.Has(v1beta1.FinalizerServiceCatalog) {
		fatalf(t, "Expected Service Catalog finalizer but was not there")
	}
}

// assertListRestrictions compares expected Fields / Labels on a list options.
func assertListRestrictions(t *testing.T, e, a clientgotesting.ListRestrictions) {
	if el, al := e.Labels.String(), a.Labels.String(); el != al {
		fatalf(t, "ListRestrictions.Labels don't match, expected %q got %q", el, al)
	}
	if ef, af := e.Fields.String(), a.Fields.String(); ef != af {
		fatalf(t, "ListRestrictions.Fields don't match, expected %q got %q", ef, af)
	}
}

func assertNumberOfBrokerActions(t *testing.T, actions []fakeosb.Action, number int) {
	if e, a := number, len(actions); e != a {
		t.Logf("%+v\n", actions)
		fatalf(t, "Unexpected number of actions: expected %v, got %v\nactions: %+v", e, a, actions)
	}
}

func noFakeActions() fakeosb.FakeClientConfiguration {
	return fakeosb.FakeClientConfiguration{}
}

func assertGetCatalog(t *testing.T, action fakeosb.Action) {
	if e, a := fakeosb.GetCatalog, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}
}

func assertProvision(t *testing.T, action fakeosb.Action, request *osb.ProvisionRequest) {
	if e, a := fakeosb.ProvisionInstance, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in provision request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertUpdateInstance(t *testing.T, action fakeosb.Action, request *osb.UpdateInstanceRequest) {
	if e, a := fakeosb.UpdateInstance, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in update instance request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertDeprovision(t *testing.T, action fakeosb.Action, request *osb.DeprovisionRequest) {
	if e, a := fakeosb.DeprovisionInstance, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in deprovision request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertPollLastOperation(t *testing.T, action fakeosb.Action, request *osb.LastOperationRequest) {
	if e, a := fakeosb.PollLastOperation, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in last operation request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertPollBindingLastOperation(t *testing.T, action fakeosb.Action, request *osb.BindingLastOperationRequest) {
	if e, a := fakeosb.PollBindingLastOperation, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in binding last operation request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertBind(t *testing.T, action fakeosb.Action, request *osb.BindRequest) {
	if e, a := fakeosb.Bind, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	actualRequest, ok := action.Request.(*osb.BindRequest)
	if !ok {
		fatalf(t, "unexpected request type; expected %T, got %T", request, actualRequest)
	}

	expectedOriginatingIdentity := request.OriginatingIdentity
	actualOriginatingIdentity := actualRequest.OriginatingIdentity
	request.OriginatingIdentity = nil
	actualRequest.OriginatingIdentity = nil

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in bind request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}

	request.OriginatingIdentity = expectedOriginatingIdentity
	actualRequest.OriginatingIdentity = actualOriginatingIdentity

	assertOriginatingIdentity(t, expectedOriginatingIdentity, actualOriginatingIdentity)
}

func assertUnbind(t *testing.T, action fakeosb.Action, request *osb.UnbindRequest) {
	if e, a := fakeosb.Unbind, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in bind request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func assertGetBinding(t *testing.T, action fakeosb.Action, request *osb.GetBindingRequest) {
	if e, a := fakeosb.GetBinding, action.Type; e != a {
		fatalf(t, "unexpected action type; expected %v, got %v", e, a)
	}

	// TODO(mkibbe): Currently the GetBinding fake has a bug where it does not
	// store the request. Re-enable this once this is fixed.
	/*
		if e, a := request, action.Request; !reflect.DeepEqual(e, a) {
			fatalf(t, "unexpected diff in GET binding request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
		}
	*/
}

func assertOriginatingIdentity(t *testing.T, expected *osb.OriginatingIdentity, actual *osb.OriginatingIdentity) {
	if e, a := expected, actual; (e != nil) != (a != nil) {
		fatalf(t, "unexpected originating identity in request: expected %q, got %q", e, a)
	}
	if expected == nil {
		return
	}
	if e, a := expected.Platform, actual.Platform; e != a {
		fatalf(t, "invalid originating identity platform in request: expected %q, got %q", e, a)
	}
	var expectedValue interface{}
	if err := json.Unmarshal([]byte(expected.Value), &expectedValue); err != nil {
		fatalf(t, "invalid originating identity value in expected request: %q: %v", expected.Value, err)
	}
	var actualValue interface{}
	if err := json.Unmarshal([]byte(actual.Value), &actualValue); err != nil {
		fatalf(t, "invalid originating identity value in actual request: %q: %v", actual.Value, err)
	}
	if e, a := expectedValue, actualValue; !reflect.DeepEqual(e, a) {
		fatalf(t, "unexpected diff in originating identity value in request: %v\nexpected %+v\ngot      %+v", diff.ObjectReflectDiff(e, a), e, a)
	}
}

func getTestCatalogConfig() fakeosb.FakeClientConfiguration {
	return fakeosb.FakeClientConfiguration{
		CatalogReaction: &fakeosb.CatalogReaction{
			Response: getTestCatalog(),
		},
	}
}

func getTestNamespacedCatalogConfig() fakeosb.FakeClientConfiguration {
	return fakeosb.FakeClientConfiguration{
		CatalogReaction: &fakeosb.CatalogReaction{
			Response: getTestNamespacedCatalog(),
		},
	}
}

func addGetNamespaceReaction(fakeKubeClient *clientgofake.Clientset) {
	fakeKubeClient.AddReactor("get", "namespaces", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				UID: types.UID(testNamespaceGUID),
			},
		}, nil
	})
}

func addGetSecretNotFoundReaction(fakeKubeClient *clientgofake.Clientset) {
	fakeKubeClient.AddReactor("get", "secrets", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), action.(clientgotesting.GetAction).GetName())
	})
}

func addGetSecretReaction(fakeKubeClient *clientgofake.Clientset, secret *corev1.Secret) {
	fakeKubeClient.AddReactor("get", "secrets", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, secret, nil
	})
}

// updateObjectReactor is used to simulate real update and return updated object,
// without that fake client will return empty struct
// TODO: in future we should consider refactor of newTestController method to use servicecatalogclientset.NewSimpleClientset() instead of &servicecatalogclientset.Clientset{}
// thanks to that such methods will not be required because we will have that behaviour out-of-the-box - also other reactor like "get" etc. could be removed.
func updateObjectReactor(resource string) (string, string, clientgotesting.ReactionFunc) {
	return "update", resource, func(action clientgotesting.Action) (bool, runtime.Object, error) {
		e := action.(clientgotesting.UpdateAction)
		return true, e.GetObject(), nil
	}
}
