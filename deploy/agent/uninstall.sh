#!/bin/bash

kubectl delete -f kindling-deploy.yml
kubectl delete cm kindlingcfg -n kindling-hxy
kubectl delete cm camera-front-config -n kindling-hxy
kubectl delete -f kindling-clusterrolebinding.yml
kubectl delete serviceaccount kindling-agent -nkindling-hxy
