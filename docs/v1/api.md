# V1 API

This document contains the Kubernetes resource types for the v1 service catalog.

# Resource Types

This section lists descriptions of the resources in the service catalog API.

_ __Note:__ All names herein are tentative, and should be considered placeholders
and used as placeholders for purposes of discussion only. This note will be
removed when names are finalized._

## `ServiceClass`

This resource indicates a general kind of backing service that a consumer
may request. The 'service kind' concept is purposefully arbitrary. We expect
each cluster operator team to assign specific meaning to the ones they choose.

## `ServiceInstance`

This resource indicates that a request by a consumer for a usable `ServiceClass`
has been successfully executed. Consumers may reference these resources to
begin using the backing service it represents.

## `ServiceClaim`

This resource is used by the consumer to get credentials for a backing service.

The byproducts of a successfully executed claim will be binding information
in the form of other, standard Kubernetes resources. We expect these to be
`ConfigMap`s and `Secret`s initially, but the number and types of these
resources will be implementation-dependent. The claim will contain
Kubernetes-style reference links for each Kubernetes resource that was created
upon successful execution.

Successfully executed claims will also serve as a record of an application that's
bound to a backing service.

## `ServiceProducer`

This resource represents an entity that may publish one or more service
classes into the catalog.  The `ServiceProducer` resource is global to the
catalog.
