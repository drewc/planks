(defpackage :planks.btree
  (:use :cl)
  (:export
   ;; Btrees
   #:btree
   #:update-btree
   #:btree-root
   #:do-btree
   #:btree-key< #:btree-key= #:btree-value=
   #:btree-max-node-size #:btree-unique-keys-p
   #:btree-key-type #:btree-value-type
   #:btree-node-class

   ;; Nodes
   #:btree-node

   ;; Functions
   #:btree-search #:btree-insert #:map-btree

   ;; Conditions
   #:btree-error #:btree-search-error #:btree-insertion-error
   #:btree-key-already-present-error #:btree-type-error
   #:btree-error-btree #:btree-error-key #:btree-error-value
   #:make-btree
   #:open-btree
   #:find-btree
   #:close-btree
   #:multi-btree
   #:add-multi-btree
   #:add-function-btree
   #:find-function-btree
   #:heap-btree
   #:btree-file-size))

(in-package :planks.btree)

(defgeneric btree-search (btree key &key errorp default-value)
  (:documentation
   "Returns the value (or persistent list of values, for btrees that
don't have unique keys) associated with KEY.  If the btree has
non-unique keys and no value is found, the empty list is returned.  If
the btree has unique keys and no value is found, the result depends on
the ERRORP option: if ERRORP is true, a btree-search-error is
signalled; otherwise, DEFAULT-VALUE is returned."))

(defgeneric btree-insert (btree key value &key if-exists)
  (:documentation
   "Adds an association from KEY to VALUE to a btree.

IF-EXISTS can be either :OVERWRITE (default) or :ERROR.

If the btree has unique keys (see BTREE-UNIQUE-KEYS-P) and KEY is
already associated with another (according to BTREE-VALUE=) value, the
result depends on the IF-EXISTS option: if IF-EXISTS is :OVERWRITE,
the old value is overwriten; if IF-EXISTS is :ERROR, a
BTREE-KEY-ALREADY-PRESENT-ERROR is signaled.

For btrees with non-unique keys, the IF-EXISTS option is ignored and
VALUE is just added to the list of values associated with KEY (unless
VALUE is already associated with KEY; in that case nothing
happens)."))

(defgeneric map-btree (btree function
                             &key min max include-min include-max order)
  (:documentation
   "Calls FUNCTION for all key/value associations in the btree where
key is in the specified interval (this means that FUNCTION can be
called with the same key more than once for btrees with non-unique
keys). FUNCTION must be a binary function; the first argument is the
btree key, the second argument is an associated value.

MIN, MAX, INCLUDE-MIN and INCLUDE-MAX specify the interval.  The
interval is left-open if MIN is nil, right-open if MAX is nil.  The
interval is inclusive on the left if INCLUDE-MIN is true (and
exclusive on the left otherwise).  The interval is inclusive on the
right if INCLUDE-MAX is true (and exclusive on the right otherwise).

ORDER is either :ASCENDING (default) or :DESCENDING."))


(defgeneric update-btree  (btree &key key value &allow-other-keys)
  (:documentation 
   "This is the function that implements the functional b+tree. It is not meant to be called by users, but is specialized when extending"))

(define-condition btree-error (error)
  ((btree :initarg :btree :reader btree-error-btree)))

(define-condition btree-search-error (btree-error)
  ((key :initarg :key :reader btree-error-key))
  (:report (lambda (condition stream)
             (format stream "An entry for the key ~S could not be found."
                     (btree-error-key condition)))))


(define-condition btree-insertion-error (btree-error)
  ((key :initarg :key :reader btree-error-key)
   (value :initarg :value :reader btree-error-value)))

(define-condition btree-key-already-present-error (btree-insertion-error)
  ()
  (:report (lambda (condition stream)
             (format stream "There's already another value for the key ~S."
                     (btree-error-key condition)))))

(define-condition btree-type-error (btree-error type-error)
  ())

