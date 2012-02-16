package backtype.storm.utils;

import java.io.PrintStream;
import java.io.IOException; 

class TreapNode <K extends Comparable<? super K>,
		 P extends Comparable<? super P>>
{
    public K key;
    public P priority;
    public TreapNode<K, P> left;
    public TreapNode<K, P> right;
    public TreapNode<K, P> parent;

    public TreapNode(K k, P p)
    {
	this(k, p, null, null, null);
    }

    public TreapNode(K k, P p,
		     TreapNode<K, P> l, TreapNode<K, P> r,
		     TreapNode<K, P> pr)
    {
        key = k;
        priority = p;
        left = l;
        right = r;
        parent = pr;
    }

    public int compareValues(TreapNode<K, P> t)
    {
        return (t == null ? -1 : key.compareTo(t.key));
    }

    public int compareValues(K k)
    {
        return key.compareTo(k);
    }

    public int comparePriorities(TreapNode<K, P> t)
    {
        return (t == null ? -1 : priority.compareTo(t.priority));
    }

    public int comparePriorities(P p)
    {
        return priority.compareTo(p);
    }

    public String toString()
    {
        return "[" + key + ", " + priority + " <"
            + (parent != null ? parent.key : "_") + ">]";
    }
}


 public class Treap <K extends Comparable<? super K>,
		     P extends Comparable<? super P>>
 {
     private TreapNode<K, P> root;

     public Treap()
     {
	 root = null;
     }

     public K top()
     {
	 return root != null ? root.key : null;
     }

     public void insert(K k, P p)
     {
	 if (root == null)
	     root = new TreapNode<K, P>(k, p, null, null, null);
	 else
	     root = insert(k, p, root);
     }

     public void remove(K k)
     {
	 if (root != null) {
	     root = remove(k, root);
	 }
     }

     public P find(K k)
     {
	 TreapNode<K, P> node = find(k, root);
	 return node != null ? node.priority : null;
     }

     public void updatePriority(K k, P p)
     {
	 if (root != null) {
	     root = updatePriority(k, p, root);
	 }
     }

     private TreapNode<K, P> insert(K k, P p, TreapNode<K, P> rt)
     {
	 int comp = rt.compareValues(k);

	 if (comp > 0) {
	     if (rt.left == null) {
		 rt.left = new TreapNode<K, P>(k, p, null, null, rt);
	     }
	     else {
		 rt.left = insert(k, p, rt.left);
	     }

	     if (rt.comparePriorities(rt.left) <= 0) {
		 rt = rotateRight(rt);
	     }
	 }
	 else if (comp < 0) {
	     if (rt.right == null) {
		 rt.right = new TreapNode<K, P>(k, p, null, null, rt);
	     }
	     else {
		 rt.right = insert(k, p, rt.right);
	     }

	     if (rt.comparePriorities(rt.right) <= 0) {
		 rt = rotateLeft(rt);
	     }
	 }

	 return rt;
     }

     private TreapNode<K, P> updatePriority(K k, P p, TreapNode<K, P> rt)
     {
	 int comp = rt.compareValues(k);

	 if (comp > 0) {
	     if (rt.left == null) return null;
	     else rt.left = updatePriority(k, p, rt.left);

	     if (rt.comparePriorities(rt.left) <= 0) rt = rotateRight(rt);
	 }
	 else if (comp < 0) {
	     if (rt.right == null) return null;
	     else rt.right = updatePriority(k, p, rt.right);

	     if (rt.comparePriorities(rt.right) <= 0) rt = rotateLeft(rt);
	 }
	 else {
	     rt.priority = p;
	 }

	 return rt;
     }

     private TreapNode<K, P> remove(K k, TreapNode<K, P> rt)
     {
	 if (rt != null) {
	     int comp = rt.compareValues(k);
	     if (comp > 0) rt.left = remove(k, rt.left);
	     else if (comp < 0) rt.right = remove(k, rt.right);
	     else rt = remove(rt);
	 }
	 return rt;
     }

     private TreapNode<K ,P> remove(TreapNode<K, P> rt)
     {
	 if (rt.left == null && rt.right == null) {
	     return null;
	 }
	 else if (rt.right == null) {
	     rt = rotateRight(rt);
	     rt.right = remove(rt.right);
	     return rt;
	 }
	 else if (rt.left == null) {
	     rt = rotateLeft(rt);
	     rt.left = remove(rt.left);
	     return rt;
	 }
	 else {
	     int cmp = rt.left.priority.compareTo(rt.right.priority);

	     if (cmp >= 0) {
		 rt = rotateRight(rt);
		 rt.right = remove(rt.right);
		 return rt;
	     }
	     else {
		 rt = rotateLeft(rt);
		 rt.left = remove(rt.left);
		 return rt;
	     }
	 }
     }

     private TreapNode<K, P> rotateRight(TreapNode<K, P> rt)
     {
	 TreapNode<K, P> pivot = rt.left;
	 rt.left = pivot.right;
	 pivot.right = rt;
	 return pivot;
     }

     private TreapNode<K, P> rotateLeft(TreapNode<K, P> rt) {
	 TreapNode<K, P> pivot = rt.right;
	 rt.right = pivot.left;
	 pivot.left = rt;
	 return pivot;
     }

     private TreapNode<K, P> find(K k, TreapNode<K, P> rt)
     {
	 if (rt != null) {
	     int comp = rt.compareValues(k);
	     if (comp == 0) return rt;
	     else if (comp > 0) return find(k, rt.left);
	     else return find(k, rt.right);
	 }

	 return null;
     }

     public String toTree()
     {
	 return toTree(root, 0);
     }

     private String toTree(TreapNode<K, P> rt, int level)
     {
	 StringBuffer sb = new StringBuffer();
	 for (int i = 0; i < level; i++) sb.append(" ");
	 if (rt != null) {
	     sb.append(rt + "\n");
	     sb.append(toTree(rt.left, level+1));
	     sb.append(toTree(rt.right, level+1));
	 }
	 else sb.append("XXX\n");
	 return sb.toString();
     }

     public void toDot(String fn) throws IOException
     {
	 toDot(new PrintStream(fn));
     }

     public void toDot(PrintStream ps)
     {
	 ps.println("graph treap {");
	 ps.println("node [shape=plaintext];");
	 toDot(root, ps);
	 ps.println("}");
     }

     private String makeLabel(TreapNode<K, P> rt)
     {
	 return "\"[" + rt.key + ", " + rt.priority + "]\"";
     }

     private void toDot(TreapNode<K, P> rt, PrintStream ps)
     {
	 if (rt.left != null) {
	     ps.println(makeLabel(rt) + " -- " + makeLabel(rt.left) + ";");
	     toDot(rt.left, ps);
	 }
	 if (rt.right != null) {
	     ps.println(makeLabel(rt) + " -- " + makeLabel(rt.right) + ";");
	     toDot(rt.right, ps);
	 }
     }
/*
     public static void main(String [] args)
     {
	 int limit = Integer.parseInt(args[0]);
	 Treap<Integer, Integer> treap = new Treap<Integer, Integer>();

	 for (int i = 0; i < limit; i++) {
	     System.out.println("Inserting " + (i*10));
	     treap.insert(i*10, limit-i);
	 }
	 System.out.println(treap.toTree());
	 treap.remove(2*limit);
	 System.out.println(treap.toTree());
	 treap.remove(3*limit);
	 System.out.println(treap.toTree());

	 for (int i = 0; i < limit; i++) {
	     System.out.println("Looking up " + (i*10));
	     Integer f = treap.find(i*10);
	     System.out.println(f);
	 }

	 treap.updatePriority(5*limit, 1000);
	 System.out.println(treap.toTree());

	 for (int i = 0; i < limit; i++) {
	     System.out.println("Removing " + (i*10));
	     treap.remove(i*10);
        }

        System.out.println(treap.toTree());
    }
 * 
 */
}