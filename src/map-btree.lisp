(in-package :planks.btree)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Iterating
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod map-btree ((btree btree) function
                      &key min max include-min include-max (order :ascending))
    (map-btree-keys btree function
                    :min min
                    :max max
                    :include-min include-min
                    :include-max include-max
                    :order order))


(defmethod map-btree-keys ((btree btree) function
                           &key min max include-min include-max (order :ascending))
  (when (slot-boundp btree 'root)
    (map-btree-keys-for-node btree (slot-value btree 'root) function
                             min max include-min include-max order)))

(defgeneric map-btree-keys-for-node (btree node function
                                     min max include-min include-max order)
  (:method (btree (node null) function
            min max include-min include-max
            order)
    nil)                         
  (:method (btree node function
            min max include-min include-max
            order)
     (if (btree-node-leaf-p node)
         ;; Leaf node.
         (let ((too-small-p
                (if min
                    (if include-min
                        (lambda (key) (funcall (btree-key< btree) key min))
                      (lambda (key) (funcall (btree-key<= btree) key min)))
                  (constantly nil)))
               (too-big-p
                (if max
                    (if include-max
                        (lambda (key) (funcall (btree-key> btree) key max))
                      (lambda (key) (funcall (btree-key>= btree) key max)))
                  (constantly nil))))
           (ecase order
             (:ascending
              (loop for i below (btree-node-binding-count node)
		 for key = (btree-node-binding-key node (btree-node-binding node i))
                    ;; If the current key is too big, all remaining keys
                    ;; will also be too big.
                    while (not (funcall too-big-p key))
                    do (unless (funcall too-small-p key)
                         (funcall function key (btree-node-binding-value node (btree-node-binding node i))))))
             (:descending
              (loop for i from (1- (btree-node-binding-count node)) downto 0
                    for key = (btree-node-binding-key node (btree-node-binding node i))
                    ;; If the current key is too small, all remaining keys
                    ;; will also be too small.
                    while (not (funcall too-small-p key))
                    do (unless (funcall too-big-p key)
                         (funcall function key (btree-node-binding-value node (btree-node-binding node i))))))))
       ;; Intermediate node.
       (ecase order
         (:ascending
          (loop for i below (btree-node-binding-count node)
		 for key = (btree-node-binding-key node (btree-node-binding node i))
                ;; All child keys will be less than or equal to the current key
                ;; and greater than the key to the left (if there is one).
                ;; So if MAX is less than the left neighbour key, we're done.
                until (and max
                           (plusp i)
                           (funcall (btree-key< btree)
                                    max
                                    (btree-node-binding-key node (btree-node-binding node i))))
                ;; And if MIN is greater than the current key, we can skip this
                ;; child.
                unless (and min
                            (not (eql key 'key-irrelevant))
                            (funcall (btree-key> btree) min key))
                do (map-btree-keys-for-node btree (btree-node-binding-value node (btree-node-binding node i))
                                            function min max include-min include-max
                                            order)))
         (:descending
          (loop for i from (1- (btree-node-binding-count node)) downto 0
                for key = (node-binding-key node i)
                ;; All child keys will be less than or equal to the current key
                ;; and greater than the key to the left (if there is one).
                ;; So if MIN is greater than the current key, we're done.
                until (and min
                           (not (eql key 'key-irrelevant))
                           (funcall (btree-key> btree) min key))
                ;; And if MAX is less than the left neighbour key, we can skip
                ;; this child.
                unless (and max
                            (plusp i)
                            (funcall (btree-key< btree)
                                     max
                                     (node-binding-key node (1- i))))
                do (map-btree-keys-for-node btree (node-binding-value node i)
                                            function min max include-min include-max
                                            order)))))))


