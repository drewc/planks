(in-package :planks.btree)

(defclass function-btree (file-btree)
  ((name :accessor btree-function-name :initarg :name)))

(defmethod update-btree :around ((btree function-btree) &rest args) 
  (let ((new-btree (call-next-method)))
    (prog1 new-btree (setf (btree-function-name new-btree)
			   (btree-function-name btree)))))
           
(defclass multi-btree-file-footer (btree-footer)
  ((btrees :initform nil)))

(defclass multi-btree (single-file-btree)
  ()
  (:default-initargs :footer-class 'multi-btree-file-footer))

(defvar *current-footer*)
(defvar *current-btree*)

(defun current-footer ()
  *current-footer*)

(defun current-btree ()
  *current-btree*)

(defmethod make-btree-footer :around ((btree multi-btree) old-footer 
				      &key key value action function-name function-index-initargs 
				      &allow-other-keys)
  (let* ((new-footer (call-next-method))
	 (btrees (and old-footer 
		      (slot-value old-footer 'btrees) (remove function-name (slot-value old-footer 'btrees) :key 'btree-function-name)))
	 (*current-btree* btree)
	 (*current-footer* new-footer))
    (if (eql action :add-function)            
	(let ((function-btree (apply #'make-instance 'function-btree 
				     :pathname (btree-pathname btree) 
				     :name function-name 
				     function-index-initargs)))
	  (map-btree 
	   btree
	   (lambda (k v)
	     (loop for (fk . fv) in (funcall function-name k v)
		:do (setf function-btree (update-btree function-btree :key fk :value fv)))))
	  (setf (slot-value new-footer 'btrees) 
		(cons function-btree btrees))
	  new-footer)
	(prog1 new-footer 
	  (setf (slot-value new-footer 'btrees) 
		(loop :for btree :in btrees 
		   :collect 	
		   (loop for (fk . fv) in (funcall (btree-function-name btree) key value)
		      :for b = (btree-insert btree fk fv)
		      :then (btree-insert b fk fv)
		      :finally (return b))))))))

(defun add-function-btree (btree function-name &rest args)
  (update-btree btree :action :add-function :function-name function-name :function-index-initargs args))

(defun find-function-btree (btree function-name)
  (find function-name (slot-value (btree-file-footer btree) 'btrees)
	:key 'btree-function-name))
 
(defmethod btree-search :around ((btree multi-btree) key &key (errorp t) (default-value nil) (function nil))
  (if function
      (btree-search (find function (slot-value (btree-file-footer btree) 'btrees)
			  :key #'btree-function-name)
		    key :errorp errorp :default-value default-value)
      (call-next-method)))


  
  