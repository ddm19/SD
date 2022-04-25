public class Tuple<X, Y> {
    public X x;
    public Y y;

    public Tuple() {
    }

    ;

    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public void set(X x, Y y) {
        this.x = x;
        this.y = y;
    }
}
