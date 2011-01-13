(in-package :planks.btree)

(defclass btree-class (standard-class)
  ((pathname :accessor btree-class-pathname
	     :initarg :btree-path)))

(defmethod btree-class-pathname :around ((class standard-class))
  (let ((path (call-next-method)))
    (if (consp path) (car path) path)))

(defmethod closer-mop:validate-superclass ((class btree-class) sc) t)

(defclass btree-object ()
  ((id :accessor btree-object-id :initarg :id)))

(defmethod btree-pathname (object)
  (btree-class-pathname (class-of object)))

(defmethod shared-initialize :around 
    ((object btree-object) slots &rest initargs 
     &key (btree (btree-pathname object)))
  (declare (ignore slots initargs))
  (let* ((parent (find-btree btree))
	 (lock (btree-lock parent)))
    (bt:with-recursive-lock-held (lock)
	(let ((id (btree-footer-next-id (btree-file-footer parent))))
	  (setf (btree-object-id object) 
		id)
	  (update-btree parent :key id :value (call-next-method) :action :make-instance)))))
  
(defmethod initialize-instance :around
  ((class btree-class) &rest initargs
   &key direct-superclasses)
  (if (loop for superclass in direct-superclasses
            thereis (closer-mop:subclassp superclass 'btree-object))
    (call-next-method)
    (apply #'call-next-method class
           :direct-superclasses
           (append direct-superclasses
                   (list (find-class 'btree-object)))
           initargs)))

(defmethod reinitialize-instance :around
  ((class btree-class) &rest initargs
   &key (direct-superclasses () direct-superclasses-p))
  (if direct-superclasses-p
    (if (loop for superclass in direct-superclasses
              thereis (closer-mop:subclassp superclass 'btree-object))
      (call-next-method)
      (apply #'call-next-method class
             :direct-superclasses
             (append direct-superclasses
                     (list (find-class 'btree-object)))
             initargs))
    (call-next-method)))