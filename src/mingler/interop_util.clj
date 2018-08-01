(ns mingler.interop-util
  "Utilities to help Java interop with MongoDB Java driver")

; Implemented as macro to keep stack trace more accurate:
(defmacro error! [message]
  `(throw (ex-info ~message {})))

; Implemented as macro to keep stack trace more accurate:
(defmacro get! [m k]
  `(or (get ~m ~k)
       (throw (ex-info (str "unknown key: " ~k) {}))))

(defn cast!
  "Convert `value` to instance of `type` using fixed set of options If the `value` is
  already instance of `type`, the `value` is returned, otherwise the `options` is
  expected to contain a mapping from `value` to instance. If mapping is not available
  throws an exception."
  [^Class type value options]
  (cond
    (nil? value) nil
    (instance? type value) value
    :else (get! options value)))

;;
;; Builing Java objects. Java driver has two kind of options classes. First, some
;; classes have setters that mutate the fields of the object. Secondly, some
;; options types are immutable and have a builder. Following functions and macros
;; help building both types using a map of setter functions.
;;

(defn reduce-with-setters
  "Given the target object, a mapping from keys to setter functions, and a map
  of options, calls setters with target and value. Takes each key/value from `options`,
  gets a setter from `setters` using the `key` (if not found throws an exception), then
  invokes the setter with the `target` and `value`. Returns target."
  [target setters options]
  (reduce-kv (fn [target k v]
               (let [setter (get! setters k)]
                 (setter target v))
               target)
             target
             options))

(defmacro apply-setters
  "If `options` is already an instance of `target-type`, returns `options`. Otherwise
  creates an instance of `target-type` and applies `reduce-with-setters` with that instance,
  `setters` and `options`. Returns the created instance."
  [^Class target-type setters options]
  `(let [setters# ~setters
         options# ~options]
     (if (instance? ~target-type options#)
       options#
       (reduce-with-setters (new ~target-type) setters# options#))))

(defmacro apply-builder
  "If `options` is already an instance of `target-type`, returns `options`. Otherwise
  creates an instance of `target-type` builder (assumes that the `target-type` has a
  static function `builder` for that purpose). Then applies `reduce-with-setters` with
  builder, `setters` and `options`. Returns an instance create by calling method `build`
  of builder."
  [^Class target-type setters options]
  `(let [setters# ~setters
         options# ~options]
     (if (instance? ~target-type options#)
       options#
       (let [builder# (. ~target-type ~'builder)]
         (reduce-with-setters builder# setters# options#)
         (. builder# build)))))
