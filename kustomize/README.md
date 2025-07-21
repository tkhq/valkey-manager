This kustomization is in two pieces:
 - a component, which is used to patch a StatefulSet
 - a kustomization resource, which is used to create the required ServiceAccount, Role, and RoleBinding

The reason for this is that Components are not able include resources and Kustomizations need to have their patch references available.

You can use these by adding the following to your own kustomization:

```yaml
resources:
  - github.com/tkhq/valkey-manager/kustomize/resource?ref=921542ba08cb716f7405dcd24f0dbb4f1976e866  # branch=main
components:
  - github.com/tkhq/valkey-manager/kustomize/component?ref=921542ba08cb716f7405dcd24f0dbb4f1976e866 # branch=main
```
