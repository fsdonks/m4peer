(ns m4peer.patch
  (:require [marathon.analysis.random]
            [hazeldemo.client :as hd]
            [clojure.core.async :as a :refer [<!!]]))

;;now let's wrap marathon.analysis.random and get it working
;;with dmap! .

(in-ns 'marathon.analysis.util)

;;this is not serializable.  maybe a record would be.
(defrecord generator [^java.util.Random gen]
  clojure.lang.IFn
  (invoke [this]    (.nextDouble gen))
  (invoke [this n]  (* (.nextDouble gen) n))
  (applyTo [this args]
    (if-let [n (some-> args seq first)]
      (.invoke this n)
      (.invoke this)))
  IGen
  (next-long   [g] (.nextLong gen))
  (next-double [g] (.nextDouble gen)))

(defn ->gen [seed]
  (let [^java.util.Random gen (java.util.Random. (long seed))]
    (->generator gen)))

(def default-gen (->gen 42))

(in-ns 'marathon.analysis.random)

(require '[hazeldemo.client :as hd])
(require '[clojure.core.async :as async])

(def ^:dynamic *run-site* :local)
;;slight api change, we were just inlining this runnnig locally
;;because no serialization.  Now we compute the rep-seed outside
;;and pass it along as data.  Also we now take a map to simplify life
;;vs vector args (simpler for the cluster side too).
(defn supply-experiment [{:keys [src phases seed->randomizer idx proj rep-seed]}]
  (-> proj
      (assoc :rep-seed rep-seed
             :supply-record-randomizer
             (seed->randomizer rep-seed))
      (try-fill src idx phases)))

(defn exec-experiments [xs]
  (case *run-site*
    :local   (util/pmap! *threads* supply-experiment xs)
    :cluster (->> (hd/dmap! marathon.analysis.random/supply-experiment xs)
                  (async/into [])
                  (async/<!!))
    (throw (ex-info "unknown *run-site*" {:in *run-site*}))))

(defn rand-target-model
  "Uses the target-model-par-av function from the marathon.analysis.experiment
  namespace as a base. This function is modified to perform a random run for
  each level of supply."
  [proj & {:keys [phases lower upper levels gen seed->randomizer]
           :or   {lower 0 upper 1 gen util/default-gen
                  seed->randomizer (fn [_] identity)}}]
   (let [project->experiments *project->experiments*]
     (->> (assoc proj :phases phases :lower lower :upper upper :levels levels
                 :gen gen  :seed->randomizer seed->randomizer)
          (e/split-project)
          (reduce
           (fn [acc [src proj]]
             (let [experiments (project->experiments proj lower upper)]
               (into acc
                     (filter (fn blah [x] (not (:error x))))
                     (exec-experiments
                       (map-indexed (fn [idx proj]
                                      {:src src
                                       :phases phases
                                       :seed->randomizer seed->randomizer
                                       :idx idx
                                       :proj proj
                                       :rep-seed (util/next-long gen)})
                                    experiments))))) [])
          (apply concat)
          vec)))

(in-ns 'm4peer.patch)

#_
(defn rand-target-model
  "Uses the target-model-par-av function from the marathon.analysis.experiment
  namespace as a base. This function is modified to perform a random run for
  each level of supply."
  [proj & {:keys [phases lower upper levels gen seed->randomizer]
           :or   {lower 0 upper 1 gen util/default-gen
                  seed->randomizer (fn [_] identity)}}]
   (let [project->experiments *project->experiments*]
     (->> (assoc proj :phases phases :lower lower :upper upper :levels levels
                 :gen gen  :seed->randomizer :seed->randomizer)
          (e/split-project)
          (reduce
           (fn [acc [src proj]]
             (let [experiments (project->experiments proj lower upper)]
               (into acc
                     (filter (fn blah [x] (not (:error x))))
                     (util/pmap! *threads*
                                 (fn experiment [[idx proj]]
                                   (let [rep-seed   (util/next-long gen)]
                                     (-> proj
                                         (assoc :rep-seed rep-seed
                                                :supply-record-randomizer
                                                (seed->randomizer rep-seed))
                                         (try-fill src idx phases))))
                                 (map-indexed vector experiments))))) [])
          (apply concat)
          vec)))
