(ns m4peer.patch
  (:require [marathon.analysis.random]
            [hazeldemo.client :as hd]))

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
    :cluster (hd/dmap! marathon.analysis.random/supply-experiment xs)
    (throw (ex-info "unknown *run-site*" {:in *run-site*}))))

(defn passthrough [_] identity)

(defn rand-target-model
  "Uses the target-model-par-av function from the marathon.analysis.experiment
  namespace as a base. This function is modified to perform a random run for
  each level of supply."
  [proj & {:keys [phases lower upper levels gen seed->randomizer]
           :or   {lower 0 upper 1 gen util/default-gen
                  seed->randomizer passthrough}}]
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

;;storing some state as a side-channel because it can'
;;resolve on the cluster.
(def length-seed (atom nil))
(defn +default-randomizer+ [seed]
  (default-randomizer seed @length-seed-lengths))

(defn init-randomizer! [lengths]
  (reset! length-seed lengths))

(defn rand-runs
  "Runs replications of the rand-target-model function.
   Mid-level function meant to be invoked from higher-level APIs.
   Caller may supply
   :reps - int, number of random replications
   :phases - optional, sequence of [phase from to] :: [string int int],
             derived from PeriodRecords if nil
   :lower - lower bound for the supply variation multiplier, defaut 0.
   :upper - upper bound for the supply variation multipler, default 1.
   :seed - integer, random seed to use for all the replications, default +default-seed+.
   :compo-lengths optional, map of {compo cyclelength} used for distribution
                  random initial cycletimes, default default-compo-lengths ."
  [proj & {:keys [reps phases lower upper seed levels compo-lengths seed->randomizer]
           :or   {lower 0 upper 1 seed +default-seed+
                  compo-lengths default-compo-lengths}}]
  (let [seed->randomizer (or seed->randomizer
                             (do (init-randomizer! compo-lengths)
                                 +default-randomizer+))
        gen              (util/->gen seed)
        phases           (or phases (util/derive-phases proj))]
    ;;input validation, we probably should do more of this in general.
    (assert (s/valid? ::phases phases) (s/explain-str ::phases []))
    (apply concat
           (map (fn [n] (rand-target-model proj
                            :phases phases :lower lower :upper upper
                            :gen   gen     :seed->randomizer seed->randomizer
                            :levels levels))
                 (range reps)))))

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
