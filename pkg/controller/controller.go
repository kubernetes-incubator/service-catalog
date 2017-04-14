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
	"time"

	"github.com/ghodss/yaml"
	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"

	checksum "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/checksum/versioned/v1alpha1"
	"github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1alpha1"
	"github.com/kubernetes-incubator/service-catalog/pkg/brokerapi"
	servicecatalogclientset "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/clientset/typed/servicecatalog/v1alpha1"
	informers "github.com/kubernetes-incubator/service-catalog/pkg/client/informers_generated/externalversions/servicecatalog/v1alpha1"
	listers "github.com/kubernetes-incubator/service-catalog/pkg/client/listers_generated/servicecatalog/v1alpha1"
)

// NewController returns a new Open Service Broker catalog
// controller.
func NewController(
	kubeClient kubernetes.Interface,
	serviceCatalogClient servicecatalogclientset.ServicecatalogV1alpha1Interface,
	brokerInformer informers.BrokerInformer,
	serviceClassInformer informers.ServiceClassInformer,
	instanceInformer informers.InstanceInformer,
	bindingInformer informers.BindingInformer,
	brokerClientCreateFunc brokerapi.CreateFunc,
	brokerRelistInterval time.Duration,
	osbAPIContextProfile bool,
) (Controller, error) {

	var (
		brokerLister       = brokerInformer.Lister()
		serviceClassLister = serviceClassInformer.Lister()
		instanceLister     = instanceInformer.Lister()

		controller = &controller{
			kubeClient:                kubeClient,
			serviceCatalogClient:      serviceCatalogClient,
			brokerClientCreateFunc:    brokerClientCreateFunc,
			brokerLister:              brokerLister,
			serviceClassLister:        serviceClassLister,
			instanceLister:            instanceLister,
			brokerRelistInterval:      brokerRelistInterval,
			enableOSBAPIContextProfle: osbAPIContextProfile,
		}
	)

	brokerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.brokerAdd,
		UpdateFunc: controller.brokerUpdate,
		DeleteFunc: controller.brokerDelete,
	})

	serviceClassInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.serviceClassAdd,
		UpdateFunc: controller.serviceClassUpdate,
		DeleteFunc: controller.serviceClassDelete,
	})

	instanceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.instanceAdd,
		UpdateFunc: controller.instanceUpdate,
		DeleteFunc: controller.instanceDelete,
	})

	bindingInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.bindingAdd,
		UpdateFunc: controller.bindingUpdate,
		DeleteFunc: controller.bindingDelete,
	})

	return controller, nil
}

// Controller describes a controller that backs the service catalog API for
// Open Service Broker compliant Brokers.
type Controller interface {
	// Run runs the controller until the given stop channel can be read from.
	Run(stopCh <-chan struct{})
}

// controller is a concrete Controller.
type controller struct {
	kubeClient                kubernetes.Interface
	serviceCatalogClient      servicecatalogclientset.ServicecatalogV1alpha1Interface
	brokerClientCreateFunc    brokerapi.CreateFunc
	brokerLister              listers.BrokerLister
	serviceClassLister        listers.ServiceClassLister
	instanceLister            listers.InstanceLister
	brokerRelistInterval      time.Duration
	enableOSBAPIContextProfle bool
}

// Run runs the controller until the given stop channel can be read from.
func (c *controller) Run(stopCh <-chan struct{}) {
	defer runtimeutil.HandleCrash()
	glog.Info("Starting service-catalog controller")

	<-stopCh
	glog.Info("Shutting down service-catalog controller")
}

// Broker handlers and control-loop

func (c *controller) brokerAdd(obj interface{}) {
	broker, ok := obj.(*v1alpha1.Broker)
	if broker == nil || !ok {
		return
	}

	c.reconcileBroker(broker)
}

func (c *controller) brokerUpdate(oldObj, newObj interface{}) {
	c.brokerAdd(newObj)
}

func (c *controller) brokerDelete(obj interface{}) {
	broker, ok := obj.(*v1alpha1.Broker)
	if broker == nil || !ok {
		return
	}

	glog.V(4).Infof("Received delete event for Broker %v", broker.Name)
}

// the Message strings have a terminating period and space so they can
// be easily combined with a follow on specific message.
const (
	errorFetchingCatalogReason  string = "ErrorFetchingCatalog"
	errorFetchingCatalogMessage string = "Error fetching catalog. "
	errorSyncingCatalogReason   string = "ErrorSyncingCatalog"
	errorSyncingCatalogMessage  string = "Error syncing catalog from Broker. "
	errorWithParameters         string = "ErrorWithParameters"
)

// shouldReconcileBroker determines whether a broker should be reconciled; it
// returns true unless the broker has a ready condition with status true and
// the controller's broker relist interval has not elapsed since the broker's
// ready condition became true.
func shouldReconcileBroker(broker *v1alpha1.Broker, now time.Time, relistInterval time.Duration) bool {
	if broker.DeletionTimestamp != nil || len(broker.Status.Conditions) == 0 {
		// If the deletion timestamp is set or the broker has no status
		// conditions, we should reconcile it.
		return true
	}

	// find the ready condition in the broker's status
	for _, condition := range broker.Status.Conditions {
		if condition.Type == v1alpha1.BrokerConditionReady {
			// The broker has a ready condition

			if condition.Status == v1alpha1.ConditionTrue {
				// The broker's ready condition has status true, meaning that
				// at some point, we successfully listed the broker's catalog.
				// We should reconcile the broker (relist the broker's
				// catalog) if it has been longer than the configured relist
				// interval since the broker's ready condition became true.
				return now.After(condition.LastTransitionTime.Add(relistInterval))
			}

			// The broker's ready condition wasn't true; we should try to re-
			// list the broker.
			return true
		}
	}

	// The broker didn't have a ready condition; we should reconcile it.
	return true
}

// reconcileBroker is the control-loop that reconciles a Broker.
func (c *controller) reconcileBroker(broker *v1alpha1.Broker) {
	glog.V(4).Infof("Processing Broker %v", broker.Name)

	// If the broker's ready condition is true and the relist interval has not
	// elapsed, do not reconcile it.
	if !shouldReconcileBroker(broker, time.Now(), c.brokerRelistInterval) {
		glog.V(10).Infof("Not processing Broker %v because relist interval has not elapsed since the broker became ready", broker.Name)
		return
	}

	username, password, err := getAuthCredentialsFromBroker(c.kubeClient, broker)
	if err != nil {
		s := fmt.Sprintf("Error getting broker auth credentials for broker %q: %s", broker.Name, err)
		glog.Info(s)
		c.updateBrokerCondition(broker, v1alpha1.BrokerConditionReady, v1alpha1.ConditionFalse, errorFetchingCatalogReason,
			errorFetchingCatalogMessage+s)
		return
	}

	glog.V(4).Infof("Creating client for Broker %v, URL: %v", broker.Name, broker.Spec.URL)
	brokerClient := c.brokerClientCreateFunc(broker.Name, broker.Spec.URL, username, password)

	if broker.DeletionTimestamp == nil { // Add or update
		glog.V(4).Infof("Adding/Updating Broker %v", broker.Name)
		brokerCatalog, err := brokerClient.GetCatalog()
		if err != nil {
			s := fmt.Sprintf("Error getting broker catalog for broker %q: %s", broker.Name, err)
			glog.Warning(s)
			c.updateBrokerCondition(broker, v1alpha1.BrokerConditionReady, v1alpha1.ConditionFalse, errorFetchingCatalogReason,
				errorFetchingCatalogMessage+s)
			return
		}
		glog.V(5).Infof("Successfully fetched %v catalog entries for Broker %v", len(brokerCatalog.Services), broker.Name)

		glog.V(4).Infof("Converting catalog response for Broker %v into service-catalog API", broker.Name)
		catalog, err := convertCatalog(brokerCatalog)
		if err != nil {
			s := fmt.Sprintf("Error converting catalog payload for broker %q to service-catalog API: %s", broker.Name, err)
			glog.Warning(s)
			c.updateBrokerCondition(broker, v1alpha1.BrokerConditionReady, v1alpha1.ConditionFalse, errorSyncingCatalogReason,
				errorSyncingCatalogMessage+s)
			return
		}
		glog.V(5).Infof("Successfully converted catalog payload from Broker %v to service-catalog API", broker.Name)

		for _, serviceClass := range catalog {
			glog.V(4).Infof("Reconciling serviceClass %v (broker %v)", serviceClass.Name, broker.Name)
			if err := c.reconcileServiceClassFromBrokerCatalog(broker, serviceClass); err != nil {
				s := fmt.Sprintf("Error reconciling serviceClass %q (broker %q): %s", serviceClass.Name, broker.Name, err)
				glog.Warning(s)
				c.updateBrokerCondition(broker, v1alpha1.BrokerConditionReady, v1alpha1.ConditionFalse, errorSyncingCatalogReason,
					errorSyncingCatalogMessage+s)
				return
			}

			glog.V(5).Infof("Reconciled serviceClass %v (broker %v)", serviceClass.Name, broker.Name)
		}

		c.updateBrokerCondition(broker, v1alpha1.BrokerConditionReady, v1alpha1.ConditionTrue, "FetchedCatalog", "Successfully fetched catalog from broker.")
		return
	}

	// All updates not having a DeletingTimestamp will have been handled above
	// and returned early. If we reach this point, we're dealing with an update
	// that's actually a soft delete-- i.e. we have some finalization to do.
	// Since the potential exists for a broker to have multiple finalizers and
	// since those most be cleared in order, we proceed with the soft delete
	// only if it's "our turn--" i.e. only if the finalizer we care about is at
	// the head of the finalizers list.
	// TODO: Should we use a more specific string here?
	if len(broker.Finalizers) > 0 && broker.Finalizers[0] == "kubernetes" {
		glog.V(4).Infof("Finalizing Broker %v", broker.Name)

		// Get ALL ServiceClasses. Remove those that reference this Broker.
		svcClasses, err := c.serviceClassLister.List(labels.Everything())
		if err != nil {
			c.updateBrokerCondition(
				broker,
				v1alpha1.BrokerConditionReady,
				v1alpha1.ConditionUnknown,
				"ErrorListingServiceClasses",
				"Error listing ServiceClasses",
			)
			return
		}

		// Delete ServiceClasses that are for THIS Broker.
		for _, svcClass := range svcClasses {
			if svcClass.BrokerName == broker.Name {
				err := c.serviceCatalogClient.ServiceClasses().Delete(svcClass.Name, &metav1.DeleteOptions{})
				if err != nil && !errors.IsNotFound(err) {
					s := fmt.Sprintf("Error deleting ServiceClass %q (Broker %q): %s", svcClass.Name, broker.Name, err)
					glog.Warning(s)
					c.updateBrokerCondition(
						broker,
						v1alpha1.BrokerConditionReady,
						v1alpha1.ConditionUnknown,
						"ErrorDeletingServiceClass",
						"Error deleting ServiceClass. "+s,
					)
					return
				}
			}
		}

		c.updateBrokerCondition(
			broker,
			v1alpha1.BrokerConditionReady,
			v1alpha1.ConditionFalse,
			"DeletedSuccessfully",
			"The broker was deleted successfully",
		)
		// Clear the finalizer
		c.updateBrokerFinalizers(broker, broker.Finalizers[1:])

		glog.V(5).Infof("Successfully deleted Broker %v", broker.Name)
	}
}

// reconcileServiceClassFromBrokerCatalog reconciles a ServiceClass after the
// Broker's catalog has been re-listed.
func (c *controller) reconcileServiceClassFromBrokerCatalog(broker *v1alpha1.Broker, serviceClass *v1alpha1.ServiceClass) error {
	serviceClass.BrokerName = broker.Name

	existingServiceClass, err := c.serviceClassLister.Get(serviceClass.Name)
	if errors.IsNotFound(err) {
		// An error returned from a lister Get call means that the object does
		// not exist.  Create a new ServiceClass.
		if _, err := c.serviceCatalogClient.ServiceClasses().Create(serviceClass); err != nil {
			glog.Errorf("Error creating serviceClass %v from Broker %v: %v", serviceClass.Name, broker.Name, err)
			return err
		}

		return nil
	} else if err != nil {
		glog.Errorf("Error getting serviceClass %v: %v", serviceClass.Name, err)
		return err
	}

	glog.V(5).Infof("Found existing serviceClass %v; updating", serviceClass.Name)

	// There was an existing service class -- project the update onto it and
	// update it.
	clone, err := api.Scheme.DeepCopy(existingServiceClass)
	if err != nil {
		return err
	}

	toUpdate := clone.(*v1alpha1.ServiceClass)
	toUpdate.Bindable = serviceClass.Bindable
	toUpdate.Plans = serviceClass.Plans
	toUpdate.PlanUpdatable = serviceClass.PlanUpdatable
	toUpdate.OSBTags = serviceClass.OSBTags
	toUpdate.OSBRequires = serviceClass.OSBRequires
	toUpdate.OSBMaxDBPerNode = serviceClass.OSBMaxDBPerNode
	toUpdate.OSBDashboardOAuth2ClientID = serviceClass.OSBDashboardOAuth2ClientID
	toUpdate.OSBDashboardSecret = serviceClass.OSBDashboardSecret
	toUpdate.OSBDashboardRedirectURI = serviceClass.OSBDashboardRedirectURI
	toUpdate.Description = serviceClass.Description

	if _, err := c.serviceCatalogClient.ServiceClasses().Update(toUpdate); err != nil {
		glog.Errorf("Error updating serviceClass %v from Broker %v: %v", serviceClass.Name, broker.Name, err)
		return err
	}

	return nil
}

// updateBrokerReadyCondition updates the ready condition for the given Broker
// with the given status, reason, and message.
func (c *controller) updateBrokerCondition(broker *v1alpha1.Broker, conditionType v1alpha1.BrokerConditionType, status v1alpha1.ConditionStatus, reason, message string) error {
	clone, err := api.Scheme.DeepCopy(broker)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Broker)
	newCondition := v1alpha1.BrokerCondition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}

	t := time.Now()

	if len(broker.Status.Conditions) == 0 {
		glog.Infof("Setting lastTransitionTime for Broker %q condition %q to %v", broker.Name, conditionType, t)
		newCondition.LastTransitionTime = metav1.NewTime(t)
		toUpdate.Status.Conditions = []v1alpha1.BrokerCondition{newCondition}
	} else {
		for i, cond := range broker.Status.Conditions {
			if cond.Type == conditionType && cond.Status != newCondition.Status {
				glog.Infof("Found status change for Broker %q condition %q: %q -> %q; setting lastTransitionTime to %v", broker.Name, conditionType, cond.Status, status, t)
				newCondition.LastTransitionTime = metav1.NewTime(time.Now())
				toUpdate.Status.Conditions[i] = newCondition
				break
			}
		}
	}

	glog.V(4).Infof("Updating ready condition for Broker %v to %v", broker.Name, status)
	_, err = c.serviceCatalogClient.Brokers().UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Error updating ready condition for Broker %v: %v", broker.Name, err)
	} else {
		glog.V(5).Infof("Updated ready condition for Broker %v to %v", broker.Name, status)
	}

	return err
}

// updateBrokerFinalizers updates the given finalizers for the given Broker.
func (c *controller) updateBrokerFinalizers(
	broker *v1alpha1.Broker,
	finalizers []string) error {

	clone, err := api.Scheme.DeepCopy(broker)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Broker)

	toUpdate.Finalizers = finalizers

	logContext := fmt.Sprintf("finalizers for Broker %v to %v",
		broker.Name, finalizers)

	glog.V(4).Infof("Updating %v", logContext)
	_, err = c.serviceCatalogClient.Brokers().UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Error updating %v: %v", logContext, err)
	}
	return err
}

// Service class handlers and control-loop

func (c *controller) serviceClassAdd(obj interface{}) {
	serviceClass, ok := obj.(*v1alpha1.ServiceClass)
	if serviceClass == nil || !ok {
		return
	}

	c.reconcileServiceClass(serviceClass)
}

func (c *controller) reconcileServiceClass(serviceClass *v1alpha1.ServiceClass) {
	glog.V(4).Infof("Processing ServiceClass %v", serviceClass.Name)
}

func (c *controller) serviceClassUpdate(oldObj, newObj interface{}) {
	c.serviceClassAdd(newObj)
}

func (c *controller) serviceClassDelete(obj interface{}) {
	serviceClass, ok := obj.(*v1alpha1.ServiceClass)
	if serviceClass == nil || !ok {
		return
	}

	glog.V(4).Infof("Received delete event for ServiceClass %v", serviceClass.Name)
}

// Instance handlers and control-loop

func (c *controller) instanceAdd(obj interface{}) {
	instance, ok := obj.(*v1alpha1.Instance)
	if instance == nil || !ok {
		return
	}

	c.reconcileInstance(instance)
}

func (c *controller) instanceUpdate(oldObj, newObj interface{}) {
	c.instanceAdd(newObj)
}

// reconcileInstance is the control-loop for reconciling Instances.
func (c *controller) reconcileInstance(instance *v1alpha1.Instance) {
	// Determine whether the checksum has been invalidated by a change to the
	// object.  If the instance's checksum matches the calculated checksum,
	// there is no work to do.
	//
	// We only do this if the deletion timestamp is nil, because the deletion
	// timestamp changes the object's state in a way that we must reconcile,
	// but does not affect the checksum.
	if instance.Spec.Checksum != nil && instance.DeletionTimestamp == nil {
		instanceChecksum := checksum.InstanceSpecChecksum(instance.Spec)
		if instanceChecksum == *instance.Spec.Checksum {
			glog.V(4).Infof("Not processing event for Instance %v/%v because checksum showed there is no work to do", instance.Namespace, instance.Name)
			return
		}
	}

	glog.V(4).Infof("Processing Instance %v/%v", instance.Namespace, instance.Name)

	serviceClass, err := c.serviceClassLister.Get(instance.Spec.ServiceClassName)
	if err != nil {
		s := fmt.Sprintf("Instance \"%s/%s\" references a non-existent ServiceClass %q", instance.Namespace, instance.Name, instance.Spec.ServiceClassName)
		glog.Info(s)
		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentServiceClass",
			"The instance references a ServiceClass that does not exist. "+s,
		)
		return
	}

	servicePlan := findServicePlan(instance.Spec.PlanName, serviceClass.Plans)
	if servicePlan == nil {
		s := fmt.Sprintf("Instance \"%s/%s\" references a non-existent ServicePlan %q on ServiceClass %q", instance.Namespace, instance.Name, instance.Spec.PlanName, serviceClass.Name)
		glog.Warning(s)
		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentServicePlan",
			"The instance references a ServicePlan that does not exist. "+s,
		)
		return
	}

	broker, err := c.brokerLister.Get(serviceClass.BrokerName)
	if err != nil {
		s := fmt.Sprintf("Instance \"%s/%s\" references a non-existent broker %q", instance.Namespace, instance.Name, serviceClass.BrokerName)
		glog.Warning(s)
		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentBroker",
			"The instance references a Broker that does not exist. "+s,
		)
		return
	}

	username, password, err := getAuthCredentialsFromBroker(c.kubeClient, broker)
	if err != nil {
		s := fmt.Sprintf("Error getting broker auth credentials for broker %q: %s", broker.Name, err)
		glog.Info(s)
		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionFalse,
			"ErrorGettingAuthCredentials",
			"Error getting auth credentials. "+s,
		)
		return
	}

	glog.V(4).Infof("Creating client for Broker %v, URL: %v", broker.Name, broker.Spec.URL)
	brokerClient := c.brokerClientCreateFunc(broker.Name, broker.Spec.URL, username, password)

	if instance.DeletionTimestamp == nil { // Add or update
		glog.V(4).Infof("Adding/Updating Instance %v/%v", instance.Namespace, instance.Name)

		var parameters map[string]interface{}
		if instance.Spec.Parameters != nil {
			parameters, err = unmarshalParameters(instance.Spec.Parameters.Raw)
			if err != nil {
				s := fmt.Sprintf("Failed to unmarshal Instance parameters\n%s\n %s", instance.Spec.Parameters, err)
				glog.Warning(s)
				c.updateInstanceCondition(
					instance,
					v1alpha1.InstanceConditionReady,
					v1alpha1.ConditionFalse,
					errorWithParameters,
					"Error unmarshaling instance parameters. "+s,
				)
				return
			}
		}

		ns, err := c.kubeClient.Core().Namespaces().Get(instance.Namespace, metav1.GetOptions{})
		if err != nil {
			s := fmt.Sprintf("Failed to get namespace %q during instance create: %s", instance.Namespace, err)
			glog.Info(s)
			c.updateInstanceCondition(
				instance,
				v1alpha1.InstanceConditionReady,
				v1alpha1.ConditionFalse,
				"ErrorFindingNamespaceForInstance",
				"Error finding namespace for instance. "+s,
			)
			return
		}

		request := &brokerapi.CreateServiceInstanceRequest{
			ServiceID:         serviceClass.OSBGUID,
			PlanID:            servicePlan.OSBGUID,
			Parameters:        parameters,
			OrgID:             string(ns.UID),
			SpaceID:           string(ns.UID),
			AcceptsIncomplete: true,
		}
		if c.enableOSBAPIContextProfle {
			request.ContextProfile = brokerapi.ContextProfile{
				Platform:  brokerapi.ContextProfilePlatformKubernetes,
				Namespace: instance.Namespace,
			}
		}

		// TODO: handle async provisioning

		glog.V(4).Infof("Provisioning a new Instance %v/%v of ServiceClass %v at Broker %v", instance.Namespace, instance.Name, serviceClass.Name, broker.Name)
		response, err := brokerClient.CreateServiceInstance(instance.Spec.OSBGUID, request)
		if err != nil {
			s := fmt.Sprintf("Error provisioning Instance \"%s/%s\" of ServiceClass %q at Broker %q: %s", instance.Namespace, instance.Name, serviceClass.Name, broker.Name, err)
			glog.Warning(s)
			c.updateInstanceCondition(
				instance,
				v1alpha1.InstanceConditionReady,
				v1alpha1.ConditionFalse,
				"ProvisionCallFailed",
				"Provision call failed. "+s)
			return
		}
		glog.V(5).Infof("Successfully provisioned Instance %v/%v of ServiceClass %v at Broker %v: response: %v", instance.Namespace, instance.Name, serviceClass.Name, broker.Name, response)

		// TODO: process response

		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionTrue,
			"ProvisionedSuccessfully",
			"The instance was provisioned successfully",
		)
		return
	}

	// All updates not having a DeletingTimestamp will have been handled above
	// and returned early. If we reach this point, we're dealing with an update
	// that's actually a soft delete-- i.e. we have some finalization to do.
	// Since the potential exists for an instance to have multiple finalizers and
	// since those most be cleared in order, we proceed with the soft delete
	// only if it's "our turn--" i.e. only if the finalizer we care about is at
	// the head of the finalizers list.
	// TODO: Should we use a more specific string here?
	if len(instance.Finalizers) > 0 && instance.Finalizers[0] == "kubernetes" {
		glog.V(4).Infof("Finalizing Instance %v/%v", instance.Namespace, instance.Name)

		request := &brokerapi.DeleteServiceInstanceRequest{
			ServiceID:         serviceClass.OSBGUID,
			PlanID:            servicePlan.OSBGUID,
			AcceptsIncomplete: true,
		}

		// TODO: handle async deprovisioning

		glog.V(4).Infof("Deprovisioning Instance %v/%v of ServiceClass %v at Broker %v", instance.Namespace, instance.Name, serviceClass.Name, broker.Name)
		err = brokerClient.DeleteServiceInstance(instance.Spec.OSBGUID, request)
		if err != nil {
			s := fmt.Sprintf("Error deprovisioning Instance \"%s/%s\" of ServiceClass %q at Broker %q: %s", instance.Namespace, instance.Name, serviceClass.Name, broker.Name, err)
			glog.Warning(s)
			c.updateInstanceCondition(
				instance,
				v1alpha1.InstanceConditionReady,
				v1alpha1.ConditionUnknown,
				"DeprovisionCallFailed",
				"Deprovision call failed. "+s)
			return
		}

		c.updateInstanceCondition(
			instance,
			v1alpha1.InstanceConditionReady,
			v1alpha1.ConditionFalse,
			"DeprovisionedSuccessfully",
			"The instance was deprovisioned successfully",
		)
		// Clear the finalizer
		c.updateInstanceFinalizers(instance, instance.Finalizers[1:])

		glog.V(5).Infof("Successfully deprovisioned Instance %v/%v of ServiceClass %v at Broker %v", instance.Namespace, instance.Name, serviceClass.Name, broker.Name)
	}
}

func findServicePlan(name string, plans []v1alpha1.ServicePlan) *v1alpha1.ServicePlan {
	for _, plan := range plans {
		if name == plan.Name {
			return &plan
		}
	}

	return nil
}

// updateInstanceCondition updates the given condition for the given Instance
// with the given status, reason, and message.
func (c *controller) updateInstanceCondition(
	instance *v1alpha1.Instance,
	conditionType v1alpha1.InstanceConditionType,
	status v1alpha1.ConditionStatus,
	reason, message string) error {

	clone, err := api.Scheme.DeepCopy(instance)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Instance)

	newCondition := v1alpha1.InstanceCondition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}

	t := time.Now()

	if len(instance.Status.Conditions) == 0 {
		glog.Infof(`Setting lastTransitionTime for Instance "%v/%v" condition %q to %v`, instance.Namespace, instance.Name, conditionType, t)
		newCondition.LastTransitionTime = metav1.NewTime(t)
		toUpdate.Status.Conditions = []v1alpha1.InstanceCondition{newCondition}
	} else {
		for i, cond := range instance.Status.Conditions {
			if cond.Type == conditionType && cond.Status != newCondition.Status {
				glog.Infof(`Found status change for Instance "%v/%v" condition %q: %q -> %q; setting lastTransitionTime to %v`, instance.Namespace, instance.Name, conditionType, cond.Status, status, t)
				newCondition.LastTransitionTime = metav1.NewTime(t)
				toUpdate.Status.Conditions[i] = newCondition
				break
			}
		}
	}

	glog.V(4).Infof("Updating %v condition for Instance %v/%v to %v", conditionType, instance.Namespace, instance.Name, status)
	_, err = c.serviceCatalogClient.Instances(instance.Namespace).UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Failed to update condition %v for Instance %v/%v to true: %v", conditionType, instance.Namespace, instance.Name, err)
	}

	return err
}

// updateInstanceFinalizers updates the given finalizers for the given Binding.
func (c *controller) updateInstanceFinalizers(
	instance *v1alpha1.Instance,
	finalizers []string) error {

	// Get the latest version of the instance so that we can avoid conflicts
	// (since we have probably just updated the status of the instance and are
	// now removing the last finalizer).
	instance, err := c.serviceCatalogClient.Instances(instance.Namespace).Get(instance.Name, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Error getting Instance %v/%v to finalize: %v", instance.Namespace, instance.Name, err)
	}

	clone, err := api.Scheme.DeepCopy(instance)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Instance)

	toUpdate.Finalizers = finalizers

	logContext := fmt.Sprintf("finalizers for Instance %v/%v to %v",
		instance.Namespace, instance.Name, finalizers)

	glog.V(4).Infof("Updating %v", logContext)
	_, err = c.serviceCatalogClient.Instances(instance.Namespace).UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Error updating %v: %v", logContext, err)
	}
	return err
}

func (c *controller) instanceDelete(obj interface{}) {
	instance, ok := obj.(*v1alpha1.Instance)
	if instance == nil || !ok {
		return
	}

	glog.V(4).Infof("Received delete event for Instance %v/%v", instance.Namespace, instance.Name)
}

// Binding handlers and control-loop

func (c *controller) bindingAdd(obj interface{}) {
	binding, ok := obj.(*v1alpha1.Binding)
	if binding == nil || !ok {
		return
	}

	c.reconcileBinding(binding)
}

func (c *controller) bindingUpdate(oldObj, newObj interface{}) {
	c.bindingAdd(newObj)
}

func (c *controller) reconcileBinding(binding *v1alpha1.Binding) {
	// Determine whether the checksum has been invalidated by a change to the
	// object.  If the binding's checksum matches the calculated checksum,
	// there is no work to do.
	//
	// We only do this if the deletion timestamp is nil, because the deletion
	// timestamp changes the object's state in a way that we must reconcile,
	// but does not affect the checksum.
	if binding.Spec.Checksum != nil && binding.DeletionTimestamp == nil {
		bindingChecksum := checksum.BindingSpecChecksum(binding.Spec)
		if bindingChecksum == *binding.Spec.Checksum {
			glog.V(4).Infof("Not processing event for Binding %v/%v because checksum showed there is no work to do", binding.Namespace, binding.Name)
			return
		}
	}

	glog.V(4).Infof("Processing Binding %v/%v", binding.Namespace, binding.Name)

	instance, err := c.instanceLister.Instances(binding.Namespace).Get(binding.Spec.InstanceRef.Name)
	if err != nil {
		s := fmt.Sprintf("Binding \"%s/%s\" references a non-existent Instance \"%s/%s\"", binding.Namespace, binding.Name, binding.Namespace, binding.Spec.InstanceRef.Name)
		glog.Warning(s)
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentInstance",
			"The binding references an Instance that does not exist. "+s,
		)
		return
	}

	serviceClass, err := c.serviceClassLister.Get(instance.Spec.ServiceClassName)
	if err != nil {
		s := fmt.Sprintf("Binding \"%s/%s\" references a non-existent ServiceClass %q", binding.Namespace, binding.Name, instance.Spec.ServiceClassName)
		glog.Warning(s)
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentServiceClass",
			"The binding references a ServiceClass that does not exist. "+s,
		)
		return
	}

	servicePlan := findServicePlan(instance.Spec.PlanName, serviceClass.Plans)
	if servicePlan == nil {
		s := fmt.Sprintf("Instance \"%s/%s\" references a non-existent ServicePlan %q on ServiceClass %q", instance.Namespace, instance.Name, servicePlan.Name, serviceClass.Name)
		glog.Warning(s)
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentServicePlan",
			"The Binding references an Instance which references ServicePlan that does not exist. "+s,
		)
		return
	}

	broker, err := c.brokerLister.Get(serviceClass.BrokerName)
	if err != nil {
		s := fmt.Sprintf("Binding \"%s/%s\" references a non-existent Broker %q", binding.Namespace, binding.Name, serviceClass.BrokerName)
		glog.Warning(s)
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"ReferencesNonexistentBroker",
			"The binding references a Broker that does not exist. "+s,
		)
		return
	}

	username, password, err := getAuthCredentialsFromBroker(c.kubeClient, broker)
	if err != nil {
		s := fmt.Sprintf("Error getting broker auth credentials for broker %q: %s", broker.Name, err)
		glog.Warning(s)
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"ErrorGettingAuthCredentials",
			"Error getting auth credentials. "+s,
		)
		return
	}

	glog.V(4).Infof("Creating client for Broker %v, URL: %v", broker.Name, broker.Spec.URL)
	brokerClient := c.brokerClientCreateFunc(broker.Name, broker.Spec.URL, username, password)

	if binding.DeletionTimestamp == nil { // Add or update
		glog.V(4).Infof("Adding/Updating Binding %v/%v", binding.Namespace, binding.Name)

		var parameters map[string]interface{}
		if binding.Spec.Parameters != nil {
			parameters, err = unmarshalParameters(binding.Spec.Parameters.Raw)
			if err != nil {
				s := fmt.Sprintf("Failed to unmarshal Binding parameters\n%s\n %s", binding.Spec.Parameters, err)
				glog.Warning(s)
				c.updateBindingCondition(
					binding,
					v1alpha1.BindingConditionReady,
					v1alpha1.ConditionFalse,
					errorWithParameters,
					"Error unmarshaling binding parameters. "+s,
				)
				return
			}
		}

		ns, err := c.kubeClient.Core().Namespaces().Get(instance.Namespace, metav1.GetOptions{})
		if err != nil {
			s := fmt.Sprintf("Failed to get namespace %q during binding: %s", instance.Namespace, err)
			glog.Info(s)
			c.updateBindingCondition(
				binding,
				v1alpha1.BindingConditionReady,
				v1alpha1.ConditionFalse,
				"ErrorFindingNamespaceForInstance",
				"Error finding namespace for instance. "+s,
			)
			return
		}

		request := &brokerapi.BindingRequest{
			ServiceID:    serviceClass.OSBGUID,
			PlanID:       servicePlan.OSBGUID,
			Parameters:   parameters,
			AppGUID:      string(ns.UID),
			BindResource: map[string]interface{}{"app_guid": string(ns.UID)},
		}
		response, err := brokerClient.CreateServiceBinding(instance.Spec.OSBGUID, binding.Spec.OSBGUID, request)
		if err != nil {
			s := fmt.Sprintf("Error creating Binding \"%s/%s\" for Instance \"%s/%s\" of ServiceClass %q at Broker %q: %s", binding.Name, binding.Namespace, instance.Namespace, instance.Name, serviceClass.Name, broker.Name, err)
			glog.Warning(s)
			c.updateBindingCondition(
				binding,
				v1alpha1.BindingConditionReady,
				v1alpha1.ConditionFalse,
				"BindCallFailed",
				"Bind call failed. "+s)
			return
		}
		err = c.injectBinding(binding, &response.Credentials)
		if err != nil {
			s := fmt.Sprintf("Error injecting binding results for Binding \"%s/%s\": %s", binding.Namespace, binding.Name, err)
			glog.Warning(s)
			c.updateBindingCondition(
				binding,
				v1alpha1.BindingConditionReady,
				v1alpha1.ConditionFalse,
				"ErrorInjectingBindResult",
				"Error injecting bind result "+s,
			)
			return
		}
		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionTrue,
			"InjectedBindResult",
			"Injected bind result",
		)

		glog.V(5).Infof("Successfully bound to Instance %v/%v of ServiceClass %v at Broker %v", instance.Namespace, instance.Name, serviceClass.Name, broker.Name)

		return
	}

	// All updates not having a DeletingTimestamp will have been handled above
	// and returned early. If we reach this point, we're dealing with an update
	// that's actually a soft delete-- i.e. we have some finalization to do.
	// Since the potential exists for a binding to have multiple finalizers and
	// since those most be cleared in order, we proceed with the soft delete
	// only if it's "our turn--" i.e. only if the finalizer we care about is at
	// the head of the finalizers list.
	// TODO: Should we use a more specific string here?
	if len(binding.Finalizers) > 0 && binding.Finalizers[0] == "kubernetes" {
		glog.V(4).Infof("Finalizing Binding %v/%v", binding.Namespace, binding.Name)
		err = c.ejectBinding(binding)
		if err != nil {
			s := fmt.Sprintf("Error deleting secret: %s", err)
			glog.Warning(s)
			c.updateBindingCondition(
				binding,
				v1alpha1.BindingConditionReady,
				v1alpha1.ConditionUnknown,
				"ErrorEjectingBinding",
				"Error ejecting binding. "+s,
			)
			return
		}
		err = brokerClient.DeleteServiceBinding(instance.Spec.OSBGUID, binding.Spec.OSBGUID, serviceClass.OSBGUID, servicePlan.OSBGUID)
		if err != nil {
			s := fmt.Sprintf("Error unbinding Binding \"%s/%s\" for Instance \"%s/%s\" of ServiceClass %q at Broker %q: %s", binding.Name, binding.Namespace, instance.Namespace, instance.Name, serviceClass.Name, broker.Name, err)
			glog.Warning(s)
			c.updateBindingCondition(
				binding,
				v1alpha1.BindingConditionReady,
				v1alpha1.ConditionFalse,
				"UnbindCallFailed",
				"Unbind call failed. "+s)
			return
		}

		c.updateBindingCondition(
			binding,
			v1alpha1.BindingConditionReady,
			v1alpha1.ConditionFalse,
			"UnboundSuccessfully",
			"The binding was deleted successfully",
		)
		// Clear the finalizer
		c.updateBindingFinalizers(binding, binding.Finalizers[1:])

		glog.V(5).Infof("Successfully deleted Binding %v/%v of Instance %v/%v of ServiceClass %v at Broker %v", binding.Namespace, binding.Name, instance.Namespace, instance.Name, serviceClass.Name, broker.Name)
	}
}

func (c *controller) injectBinding(binding *v1alpha1.Binding, credentials *brokerapi.Credential) error {
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      binding.Spec.SecretName,
			Namespace: binding.Namespace,
		},
		Data: make(map[string][]byte),
	}

	for k, v := range *credentials {
		var err error
		secret.Data[k], err = serialize(v)
		if err != nil {
			return fmt.Errorf("Unable to serialize credential value %q: %v; %s",
				k, v, err)
		}
	}

	found := false

	_, err := c.kubeClient.Core().Secrets(binding.Namespace).Get(binding.Spec.SecretName, metav1.GetOptions{})
	if err == nil {
		found = true
	}

	if found {
		_, err = c.kubeClient.Core().Secrets(binding.Namespace).Update(secret)
	} else {
		_, err = c.kubeClient.Core().Secrets(binding.Namespace).Create(secret)
	}

	return err
}

func (c *controller) ejectBinding(binding *v1alpha1.Binding) error {
	_, err := c.kubeClient.Core().Secrets(binding.Namespace).Get(binding.Spec.SecretName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		glog.Errorf("Error getting secret %v/%v: %v", binding.Namespace, binding.Spec.SecretName, err)
		return err
	}

	glog.V(5).Infof("Deleting secret %v/%v", binding.Namespace, binding.Spec.SecretName)
	err = c.kubeClient.Core().Secrets(binding.Namespace).Delete(binding.Spec.SecretName, &metav1.DeleteOptions{})

	return err
}

// updateBindingCondition updates the given condition for the given Binding
// with the given status, reason, and message.
func (c *controller) updateBindingCondition(
	binding *v1alpha1.Binding,
	conditionType v1alpha1.BindingConditionType,
	status v1alpha1.ConditionStatus,
	reason, message string) error {

	clone, err := api.Scheme.DeepCopy(binding)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Binding)

	newCondition := v1alpha1.BindingCondition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}

	t := time.Now()

	if len(binding.Status.Conditions) == 0 {
		glog.Infof(`Setting lastTransitionTime for Binding "%v/%v" condition %q to %v`, binding.Namespace, binding.Name, conditionType, t)
		newCondition.LastTransitionTime = metav1.NewTime(t)
		toUpdate.Status.Conditions = []v1alpha1.BindingCondition{newCondition}
	} else {
		for i, cond := range binding.Status.Conditions {
			if cond.Type == conditionType && cond.Status != newCondition.Status {
				glog.Infof(`Found status change for Binding "%v/%v" condition %q: %q -> %q; setting lastTransitionTime to %v`, binding.Namespace, binding.Name, conditionType, cond.Status, status, t)
				newCondition.LastTransitionTime = metav1.NewTime(time.Now())
				toUpdate.Status.Conditions[i] = newCondition
				break
			}
		}
	}

	logContext := fmt.Sprintf("%v condition for Binding %v/%v to %v (Reason: %q, Message: %q)",
		conditionType, binding.Namespace, binding.Name, status, reason, message)

	glog.V(4).Infof("Updating %v", logContext)
	_, err = c.serviceCatalogClient.Bindings(binding.Namespace).UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Error updating %v: %v", logContext, err)
	}
	return err
}

// updateBindingFinalizers updates the given finalizers for the given Binding.
func (c *controller) updateBindingFinalizers(
	binding *v1alpha1.Binding,
	finalizers []string) error {

	// Get the latest version of the binding so that we can avoid conflicts
	// (since we have probably just updated the status of the binding and are
	// now removing the last finalizer).
	binding, err := c.serviceCatalogClient.Bindings(binding.Namespace).Get(binding.Name, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Error getting Binding %v/%v to finalize: %v", binding.Namespace, binding.Name, err)
	}

	clone, err := api.Scheme.DeepCopy(binding)
	if err != nil {
		return err
	}
	toUpdate := clone.(*v1alpha1.Binding)

	toUpdate.Finalizers = finalizers

	logContext := fmt.Sprintf("finalizers for Binding %v/%v to %v",
		binding.Namespace, binding.Name, finalizers)

	glog.V(4).Infof("Updating %v", logContext)
	_, err = c.serviceCatalogClient.Bindings(binding.Namespace).UpdateStatus(toUpdate)
	if err != nil {
		glog.Errorf("Error updating %v: %v", logContext, err)
	}
	return err
}

func (c *controller) bindingDelete(obj interface{}) {
	binding, ok := obj.(*v1alpha1.Binding)
	if binding == nil || !ok {
		return
	}

	glog.V(4).Infof("Received delete event for Binding %v/%v", binding.Namespace, binding.Name)
}

// Broker utility methods - move?

// getAuthCredentialsFromBroker returns the auth credentials, if any,
// contained in the secret referenced in the Broker's AuthSecret field, or
// returns an error. If the AuthSecret field is nil, empty values are
// returned.
func getAuthCredentialsFromBroker(client kubernetes.Interface, broker *v1alpha1.Broker) (username, password string, err error) {
	if broker.Spec.AuthSecret == nil {
		return "", "", nil
	}

	authSecret, err := client.Core().Secrets(broker.Spec.AuthSecret.Namespace).Get(broker.Spec.AuthSecret.Name, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	usernameBytes, ok := authSecret.Data["username"]
	if !ok {
		return "", "", fmt.Errorf("auth secret didn't contain username")
	}

	passwordBytes, ok := authSecret.Data["password"]
	if !ok {
		return "", "", fmt.Errorf("auth secret didn't contain password")
	}

	return string(usernameBytes), string(passwordBytes), nil
}

// convertCatalog converts a service broker catalog into an array of ServiceClasses
func convertCatalog(in *brokerapi.Catalog) ([]*v1alpha1.ServiceClass, error) {
	ret := make([]*v1alpha1.ServiceClass, len(in.Services))
	for i, svc := range in.Services {
		plans, err := convertServicePlans(svc.Plans)
		if err != nil {
			return nil, err
		}
		ret[i] = &v1alpha1.ServiceClass{
			Bindable:      svc.Bindable,
			Plans:         plans,
			PlanUpdatable: svc.PlanUpdateable,
			OSBGUID:       svc.ID,
			OSBTags:       svc.Tags,
			OSBRequires:   svc.Requires,
			Description:   &svc.Description,
			// OSBMetadata:   svc.Metadata,
		}

		if svc.Metadata != nil {
			metadata, err := json.Marshal(svc.Metadata)
			if err != nil {
				err = fmt.Errorf("Failed to marshal metadata\n%+v\n %v", svc.Metadata, err)
				glog.Error(err)
				return nil, err
			}
			ret[i].OSBMetadata = &runtime.RawExtension{Raw: metadata}
		}

		ret[i].SetName(svc.Name)
	}
	return ret, nil
}

func convertServicePlans(plans []brokerapi.ServicePlan) ([]v1alpha1.ServicePlan, error) {
	ret := make([]v1alpha1.ServicePlan, len(plans))
	for i, plan := range plans {
		ret[i] = v1alpha1.ServicePlan{
			Name:    plan.Name,
			OSBGUID: plan.ID,
			// OSBMetadata: plan.Metadata,
			OSBFree:     plan.Free,
			Description: &plan.Description,
		}
		if plan.Metadata != nil {
			metadata, err := json.Marshal(plan.Metadata)
			if err != nil {
				err = fmt.Errorf("Failed to marshal metadata\n%+v\n %v", plan.Metadata, err)
				glog.Error(err)
				return nil, err
			}
			ret[i].OSBMetadata = &runtime.RawExtension{Raw: metadata}
		}

	}
	return ret, nil
}

func unmarshalParameters(in []byte) (map[string]interface{}, error) {
	parameters := make(map[string]interface{})
	if len(in) > 0 {
		if err := yaml.Unmarshal(in, &parameters); err != nil {
			return parameters, err
		}
	}
	return parameters, nil
}
