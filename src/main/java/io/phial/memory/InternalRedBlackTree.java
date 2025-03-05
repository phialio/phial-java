package io.phial.memory;

public class InternalRedBlackTree {
    private static final boolean RED = false;
    private static final boolean BLACK = true;

    private static class Node {
        Node parent;
        Node left;
        Node right;
        long key;
        boolean color;

        public Node(Node parent, long key) {
            this.parent = parent;
            this.key = key;
        }

        @Override
        public String toString() {
            return treeString(this, "");
        }

        private static String treeString(Node node, String prefix) {
            var result = prefix + nodeString(node);
            if (node.left != null) {
                result += "\n" + treeString(node.left, prefix + "  ");
            }
            if (node.right != null) {
                result += "\n\n" + treeString(node.right, prefix + "  ");
            }
            return result;
        }

        private static String nodeString(Node node) {
            return String.format("%d(%s)", node.key, node.color ? "BLACK" : "RED");
        }
    }

    private int size = 0;
    private Node root;
    private Node first;

    public void validate() {
        validate(this.root);
        if (this.size != count(this.root)) {
            System.out.println(this.size);
            System.out.println(this.root);
            throw new Error("invalid size");
        }
        var node = this.root;
        while (node != null && node.left != null) {
            node = node.left;
        }
        if (node != this.first) {
            System.out.println(this.root);

            throw new Error("invalid first");
        }
    }

    private static int count(Node node) {
        if (node == null) {
            return 0;
        }
        return count(node.left) + count(node.right) + 1;
    }

    private static int validate(Node node) {
        if (node == null) {
            return 1;
        }
        int l = validate(node.left);
        int r = validate(node.right);
        if (l != r) {
            throw new Error(l + "<>" + r + " " + node.toString());
        }
        if (node.left != null) {
            if (node.color == RED && node.left.color == RED) {
                throw new Error("left red conflict " + node);
            }
            if (node.left.key >= node.key) {
                throw new Error("left key greater " + node);
            }
            if (node.left.parent != node) {
                throw new Error("left parent " + node);
            }
        }
        if (node.right != null) {
            if (node.color == RED && node.right.color == RED) {
                throw new Error("right red conflict " + node);
            }
            if (node.right.key <= node.key) {
                throw new Error("right key less " + node);
            }
            if (node.right.parent != node) {
                throw new Error("right parent " + node);
            }
        }
        return l + (node.color == RED ? 0 : 1);
    }


    public int size() {
        return this.size;
    }

    public long pollFirst() {
        if (this.first == null) {
            return 0;
        }
        long result = this.first.key;
        this.doRemove(this.first);
        //this.validate();
        return result;
    }

    public void add(long key) {
        this.addOrRemoveBuddy(key, 0);
    }

    public int addOrRemoveBuddy(long key, long buddy) {
        var node = this.root;
        if (node == null) {
            this.root = new Node(null, key);
            this.root.color = BLACK;
            this.first = this.root;
            this.size = 1;
            //this.validate();
            return 1;
        }
        Node newNode;
        Node predecessor = null;
        Node successor = null;
        for (; ; ) {
            Node next;
            if (key < node.key) {
                successor = node;
                next = node.left;
                if (next == null) {
                    if (buddy < key) {
                        if (predecessor != null && predecessor.key == buddy) {
                            this.doRemove(predecessor);
                            //this.validate();
                            return -1;
                        }
                    } else if (node.key == buddy) {
                        this.doRemove(node);
                        //this.validate();
                        return -1;
                    }
                    newNode = new Node(node, key);
                    if (key < this.first.key) {
                        this.first = newNode;
                    }
                    node.left = newNode;
                    break;
                }
            } else if (key > node.key) {
                predecessor = node;
                next = node.right;
                if (next == null) {
                    if (buddy > key) {
                        if (successor != null && successor.key == buddy) {
                            this.doRemove(successor);
                            //this.validate();
                            return -1;
                        }
                    } else if (node.key == buddy) {
                        this.doRemove(node);
                        //this.validate();
                        return -1;
                    }
                    newNode = new Node(node, key);
                    node.right = newNode;
                    break;
                }
            } else {
                //this.validate();
                return 0;
            }
            node = next;
        }
        ++this.size;
        node = newNode;
        while (node != this.root) {
            var parent = node.parent;
            if (parent.color == BLACK || parent == this.root) {
                // no need to re-balance
                break;
            }
            var grandpa = parent.parent;
            var uncle = parent == grandpa.left ? grandpa.right : grandpa.left;
            if (uncle != null && uncle.color == RED) {
                //       grandpa(BLACK,h+1)                     grandpa(RED,h+1)
                //              / \                                   / \
                // parent(RED,h)   uncle(RED,h)  ->  parent(BLACK,h+1)   uncle(BLACK,h+1)
                //      |                                   |
                //  node(RED,h)                        node(RED,h)
                parent.color = uncle.color = BLACK;
                grandpa.color = RED;
                node = grandpa;
                continue;
            }
            // make node the outer grandchild
            //
            //            grandpa(BLACK,h+1)                             grandpa(BLACK,h+1)
            //                   / \                                             / \
            //      parent(RED,h)   uncle(BLACK,h)  ->                node(RED,h)   uncle(BLACK,h)
            //           / \                                               / \
            // (BLACK1,h)   node(RED,h)                       parent(RED,h)   (BLACK3,h)
            //                 / \                                 / \
            //       (BLACK2,h)   (BLACK3,h)             (BLACK1,h)   (BLACK2,h)
            // it is not necessary to update node because we do not need it anymore
            if (node == parent.right && parent == grandpa.left) {
                this.rotateLeft(parent);
                parent = node;
            } else if (node == parent.left && parent == grandpa.right) {
                this.rotateRight(parent);
                parent = node;
            }
            //             grandpa(BLACK,h+1)               parent(BLACK,h+1)
            //                    / \                              / \
            //       parent(RED,h)   uncle(BLACK,h)  ->  node(RED,h)  grandpa(RED,h)
            //           / \                                               / \
            // node(RED,h)  (BLACK,h)                              (BLACK,h)  uncle(BLACK,h)
            if (parent == grandpa.left) {
                this.rotateRight(grandpa);
            } else {
                this.rotateLeft(grandpa);
            }
            parent.color = BLACK;
            grandpa.color = RED;
            //this.validate();
            return 1;
        }
        this.root.color = BLACK;
        //this.validate();
        return 1;
    }

    public boolean containsRange(long start, long end, long gap) {
        return containsRange(this.root, start, end, gap);
    }

    public boolean remove(long key) {
        var node = this.root;
        while (node != null) {
            if (key < node.key) {
                node = node.left;
            } else if (key > node.key) {
                node = node.right;
            } else {
                break;
            }
        }
        if (node == null) {
            // not found
            return false;
        }
        this.doRemove(node);
        //this.validate();
        return true;
    }

    private static boolean containsRange(Node node, long start, long end, long gap) {
        if (start > end) {
            return true;
        }
        while (node != null && node.key < start) {
            node = node.right;
        }
        while (node != null && node.key > end) {
            node = node.left;
        }
        if (node == null) {
            return false;
        }
        long key = node.key;
        long remainder = (key - start) % gap;
        if (remainder == 0) {
            return containsRange(node.left, start, key - gap, gap)
                    && containsRange(node.right, key + gap, end, gap);
        } else {
            return containsRange(node.left, start, key - remainder, gap)
                    && containsRange(node.right, key + gap - remainder, end, gap);
        }
    }

    private void doRemove(Node node) {
        --this.size;
        long key = node.key;

        if (node.left != null && node.right != null) {
            // 2 children, find the successor, which must has no child or only 1 child
            //     node                      successor
            //      /\                           /\
            //     *  right         -->         *  right
            //         / \                          / \
            // successor  *                     node   *      (node is to be removed, its key is meaningless)
            //         \                            \
            //          ?                            ?
            var successor = node.right;
            while (successor.left != null) {
                successor = successor.left;
            }
            node.key = successor.key;
            node = successor;
        }

        // if node has only 1 child, this child should have no child
        var left = node.left;
        if (left != null) {
            //    node           left
            //     /      -->
            //   left
            node.key = node.left.key;
            node.left = null;
            if (left == this.first) {
                this.first = node;
            }
            //this.validate();
            return;
        }
        var right = node.right;
        if (right != null) {
            //    node           right
            //      \      -->
            //     right
            node.key = right.key;
            node.right = null;
            //this.validate();
            return;
        }

        // no child since here
        if (node == this.root) {
            // make tree empty
            this.root = null;
            this.first = null;
            //this.validate();
            return;
        }
        var parent = node.parent;
        if (node == this.first) {
            // its successor should be the parent
            this.first = parent;
        }
        if (node.color == RED) {
            // simply remove node
            InternalRedBlackTree.removeNode(node);
            //this.validate();
            return;
        } // node.color == RED

        var nodeToRemove = node;
        for (; ; ) {
            // node is always BLACK
            parent = node.parent;
            Node sibling;
            Node closeNephew;
            Node distantNephew;
            if (node == parent.left) {
                sibling = parent.right;
                closeNephew = sibling.left;
                distantNephew = sibling.right;
            } else {
                sibling = parent.left;
                closeNephew = sibling.right;
                distantNephew = sibling.left;
            }
            if (sibling.color == RED) {
                //        parent(BLACK,h+1)                                     sibling(BLACK,h+1)
                //              / \                                                    / \
                //  node(BLACK,h)  sibling(RED,h)                 ->       parent(RED,h)  distantNephew(BLACK,h)
                //                    / \                                        / \
                // closeNephew(BLACK,h)  distantNephew(BLACK,h)      node(BLACK,h)  closeNephew(BLACK,h)
                if (node == parent.left) {
                    this.rotateLeft(parent);
                } else {
                    this.rotateRight(parent);
                }
                parent.color = RED;
                sibling.color = BLACK;
                sibling = closeNephew;
                if (node == parent.left) {
                    closeNephew = sibling.left;
                    distantNephew = sibling.right;
                } else {
                    closeNephew = sibling.right;
                    distantNephew = sibling.left;
                }
            } // sibling.color == RED
            if (closeNephew != null && closeNephew.color == RED && (distantNephew == null || distantNephew.color == BLACK)) {
                //            parent                                              parent
                //              / \                                                / \
                //  node(BLACK,h)  sibling(BLACK,h)                ->  node(BLACK,h)  closeNephew(BLACK,h)
                //                   / \                                                / \
                //  closeNephew(RED,h)  distantNephew(BLACK,h-1)              (BLACK,h-1)  sibling(RED,h-1)
                //           / \                                                                 / \
                // (BLACK,h-1)  (BLACK,h-1)                                            (BLACK,h-1)  distantNephew(BLACK,h-1)
                if (node == parent.left) {
                    this.rotateRight(sibling);
                } else {
                    this.rotateLeft(sibling);
                }
                sibling.color = RED;
                closeNephew.color = BLACK;
                distantNephew = sibling;
                sibling = closeNephew;
                // now sibling is BLACK and distantNephew is RED
                // it is not necessary to update closeNephew because we do not need it in the next clause
            }
            if (distantNephew != null && distantNephew.color == RED) {
                //          parent(h+1）                                     sibling(parent's original color,h+1)
                //             / \                                                          / \
                // node(BLACK,h)  sibling(BLACK,h)               ->           parent(BLACK,h)  distantNephew(BLACK,h)
                //                   / \                                           / \
                //              (h-1)   distantNephew(RED,h-1)       node(BLACK,h-1)  (h-1)
                //
                // balanced, done
                if (node == parent.left) {
                    this.rotateLeft(parent);
                } else {
                    this.rotateRight(parent);
                }
                sibling.color = parent.color;
                parent.color = BLACK;
                distantNephew.color = BLACK;
                InternalRedBlackTree.removeNode(nodeToRemove);
                //this.validate();
                return;
            }

            // sibling and its two children are all BLACK
            if (parent.color == RED) {
                //          parent(RED,h)                                       parent(BLACK,h)
                //              / \                                                    / \
                //  node(BLACK,h)  sibling(BLACK,h)                  ->  node(BLACK,h-1)  sibling(RED,h-1)
                //                       / \                                                  / \
                //  closeNephew(BLACK,h-1)  distantNephew(BLACK,h-1)     closeNephew(BLACK,h-1)  distantNephew(BLACK,h-1)
                //
                // balanced done
                parent.color = BLACK;
                sibling.color = RED;
                InternalRedBlackTree.removeNode(nodeToRemove);
                //this.validate();
                return;
            } // parent.color == RED

            //          parent(BLACK,h+1)                                   parent(BLACK,h)
            //               / \                                                  / \
            //  node(BLACK,h)  sibling(BLACK,h)                 ->  node(BLACK,h-1)  sibling(RED,h-1)
            //                       / \                                                 / \
            //  closeNephew(BLACK,h-1)  distantNephew(BLACK,h-1)    closeNephew(BLACK,h-1)  distantNephew(BLACK,h-1)
            //
            // the parent subtree is now balanced but its black height is decreased by 1, continue the loop
            sibling.color = RED;
            node = parent;
            if (node == this.root) {
                // done
                InternalRedBlackTree.removeNode(nodeToRemove);
                //this.validate();
                return;
            }
        }
    }

    private static void removeNode(Node node) {
        var parent = node.parent;
        if (node == parent.left) {
            parent.left = null;
        } else {
            parent.right = null;
        }
    }

    private void rotateLeft(Node node) {
        var parent = node.parent;
        var right = node.right;
        var rightLeft = right.left;
        node.right = rightLeft;
        if (rightLeft != null) {
            rightLeft.parent = node;
        }
        right.parent = parent;
        if (parent == null) {
            this.root = right;
        } else if (node == parent.left) {
            parent.left = right;
        } else {
            parent.right = right;
        }
        right.left = node;
        node.parent = right;
    }

    private void rotateRight(Node node) {
        var parent = node.parent;
        var left = node.left;
        var leftRight = left.right;
        node.left = leftRight;
        if (leftRight != null) {
            leftRight.parent = node;
        }
        left.parent = parent;
        if (parent == null) {
            this.root = left;
        } else if (node == parent.right) {
            parent.right = left;
        } else {
            parent.left = left;
        }
        left.right = node;
        node.parent = left;
    }
}
