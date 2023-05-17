#!/bin/bash
kubectl create ns kindling-hxy

kubectl create serviceaccount kindling-agent -nkindling-hxy
kubectl apply -f kindling-clusterrolebinding.yml
kubectl create cm kindlingcfg -n kindling-hxy --from-file=kindling-collector-config.yml
kubectl apply -f camera-front-configmap.yml
kubectl apply -f kindling-deploy.yml