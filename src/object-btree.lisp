(in-package :planks.btree)

(defclass object-storage-btree (single-file-btree) 
  ()
  (:default-initargs :footer-class 'object-storage-footer))

(defclass object-storage-footer (btree-footer)
  ((next-id :accessor btree-footer-next-id :initform 0)))

(defgeneric make-btree-footer (btree old-footer &key &allow-other-keys))

(defmethod make-btree-footer :around ((btree object-storage-btree) old-footer &key action)
  (let ((footer (call-next-method)))
    (prog1 footer 
      (setf (btree-footer-next-id footer)
	    (if (eql :make-instance action)
		(if old-footer (1+ (btree-footer-next-id old-footer)) 0)
		(if old-footer (btree-footer-next-id old-footer) 0))))))

(defmethod update-btree ((btree object-storage-btree) &rest args 
			 &key value-thunk &allow-other-keys)
  (if value-thunk 
      (let ((value (funcall value-thunk)))
	(apply #'call-next-method btree (list* :value value (alexandria:remove-from-plist args :value-thunk))))
      (call-next-method)))

(defclass nested-btree (file-btree)
  ((pathname :initarg :btree)
   (key :initarg key)))


(defmethod btree-pathname :around  ((btree nested-btree))
  (declare (special %btree-path%))
  (if (boundp '%btree-path%) %btree-path% (call-next-method)))

(defmethod update-btree :around ((btree nested-btree) &key  &allow-other-keys)
  (let* ((%btree-path% (btree-pathname btree))
	 (parent (find-btree (btree-pathname btree))))
    (declare (special %btree-path%))
    (bt:with-recursive-lock-held ((btree-lock parent))
      (let* ((value)
	     (value-thunk 
	      (lambda () 
		(setf value 
		      (let ((value (call-next-method)))
			(prog1 value
			  #+nil (setf (btree-object-id value)
				 (btree-object-id btree))))))))
	(update-btree (find-btree (btree-pathname btree))
		      :key (btree-object-id btree)
		      :value-thunk value-thunk)
	value))))

