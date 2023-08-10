;;obe
(ns m4peer.config
  #_
  (:import [com.hazelcast.config Config clientConfig]))
#_#_
(defn ->aws [id]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    (.. cfg getNetworkConfig getJoin getMulticastConfig (setEnabled false))
    (.. cfg getNetworkConfig getJoin getAwsConfig       (setEnabled true))
    cfg))

(defn ->default [id]
  (let [cfg (Config.)]
    (.. cfg (setInstanceName id))
    cfg))

  ;;client shuld be fine.
