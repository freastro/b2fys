package net.freastro.b2fys;

import sun.awt.Mutex;
import sun.misc.ConditionLock;

class Ticket {

    int Total;

    int total;
    int outstanding;

    Mutex mu = new Mutex();
    ConditionLock cond;

    Ticket() {
    }

    Ticket(int Total) {
        this.Total = Total;
    }

    Ticket init() {
        cond = new ConditionLock();
        total = Total;
        return this;
    }
}
