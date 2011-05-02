(in-package :planks.btree)

(defclass persistent-standard-class (standard-class)
  ((btree :initarg :btree :accessor persistent-standard-class-btree
	  :accessor btree-class-pathname)))

(defmethod closer-mop:validate-superclass ((class persistent-standard-class) sc) t)

(defclass persistent-standard-object ()
  ((slot-btree :accessor persistent-standard-object-slot-btree)))
    
(defclass persistent-standard-object-slot-btree (nested-btree)
  ((class-name :accessor persistent-standard-object-slot-btree-class-name
	       :initarg :class-name))
  (:metaclass btree-class)
    (:default-initargs
   :key< 'string=
   :key= 'string=
   :key-type 'symbol))

(defmethod slot-definition-persistentp (slot)
  nil)

(defmethod update-btree ((btree persistent-standard-object-slot-btree) &key  &allow-other-keys)
  (let ((new-btree (call-next-method)))
    (prog1 new-btree
      (setf (persistent-standard-object-slot-btree-class-name new-btree)
	    (persistent-standard-object-slot-btree-class-name btree)))))
	    

(defconstant +persistent-standard-object-marker+ #xC2)

(defmethod rs::serialize ((object persistent-standard-object-slot-btree) stream)
  (rs::serialize-marker +persistent-standard-object-marker+ stream)
  (rs::serialize-standard-object object stream))

(defmethod rs::serialize ((object persistent-standard-object) stream)
  (rs::serialize (persistent-standard-object-slot-btree object) stream))

(defmethod rs::deserialize-contents ((marker (eql +persistent-standard-object-marker+)) stream)
  (let* ((btree (rs::deserialize stream))
	 (instance (allocate-instance (find-class 
				       (persistent-standard-object-slot-btree-class-name btree)))))
    (setf (persistent-standard-object-slot-btree instance)
	  btree)
    instance))

(defmethod btree-pathname ((object persistent-standard-object))
  (btree-class-pathname (class-of object)))

(defclass persistent-standard-class-slot-definition ()
  ((persistentp :accessor slot-definition-persistentp 
		:initarg :persistent :initform t)))

(defclass persistent-standard-class-direct-slot-definition 
    (persistent-standard-class-slot-definition 
     closer-mop:standard-direct-slot-definition) 
  ())

(defclass persistent-standard-class-effective-slot-definition 
    (persistent-standard-class-slot-definition 
     closer-mop:standard-effective-slot-definition) 
  ())

(defmethod closer-mop:direct-slot-definition-class
           ((class persistent-standard-class) &key &allow-other-keys)
  (find-class 'persistent-standard-class-direct-slot-definition))

(defvar *persistent-standard-class-effective-slot-definition-class*)

(defmethod closer-mop:effective-slot-definition-class
           ((class persistent-standard-class) &key &allow-other-keys)
  (if *persistent-standard-class-effective-slot-definition-class*
    *persistent-standard-class-effective-slot-definition-class*
    (call-next-method)))

(defmethod closer-mop:compute-effective-slot-definition
           ((class persistent-standard-class) name direct-slot-definitions)
  (declare (ignore name))
  (let ((*persistent-standard-class-effective-slot-definition-class*
         (when (some #'slot-definition-persistentp direct-slot-definitions)
	   (find-class 'persistent-standard-class-effective-slot-definition))))
    (call-next-method)))

(defmethod shared-initialize :before ((object persistent-standard-object) slots &rest initargs)
  (declare (ignore slots initargs))
  (setf (persistent-standard-object-slot-btree object) 
	(make-instance 'persistent-standard-object-slot-btree 
		       :btree (btree-pathname object)
		       :class-name (class-name (class-of object)))))

(defmethod closer-mop:slot-value-using-class ((class persistent-standard-class)
					      (object persistent-standard-object)
					      (slotd persistent-standard-class-effective-slot-definition))
  (btree-search (persistent-standard-object-slot-btree object) 
		(closer-mop:slot-definition-name slotd)))

(defmethod (setf closer-mop:slot-value-using-class) (value 
						     (class persistent-standard-class)
						     (object persistent-standard-object)
						     (slotd persistent-standard-class-effective-slot-definition))
  
  (prog1 value
    (setf (persistent-standard-object-slot-btree object)
	  (btree-insert (persistent-standard-object-slot-btree object) 
		 	(closer-mop:slot-definition-name slotd)
			 value))))

(defmethod closer-mop:slot-boundp-using-class 
    ((class persistent-standard-class)
     (object persistent-standard-object)
     (slotd persistent-standard-class-effective-slot-definition))
  (btree-search (persistent-standard-object-slot-btree object) 
		(closer-mop:slot-definition-name slotd)
		:errorp nil))
  
(defmethod initialize-instance :around
  ((class persistent-standard-class) &rest initargs
   &key direct-superclasses)
  (if (loop for superclass in direct-superclasses
            thereis (closer-mop:subclassp superclass 'persistent-standard-object))
    (call-next-method)
    (apply #'call-next-method class
           :direct-superclasses
           (append direct-superclasses
                   (list (find-class 'persistent-standard-object)))
           initargs)))

(defmethod reinitialize-instance :around
  ((class persistent-standard-class) &rest initargs
   &key (direct-superclasses () direct-superclasses-p))
  (if direct-superclasses-p
    (if (loop for superclass in direct-superclasses
              thereis (closer-mop:subclassp superclass 'persistent-standard-object))
      (call-next-method)
      (apply #'call-next-method class
             :direct-superclasses
             (append direct-superclasses
                     (list (find-class 'persistent-standard-object)))
             initargs))
    (call-next-method)))