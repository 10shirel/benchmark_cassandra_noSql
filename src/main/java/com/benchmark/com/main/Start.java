package com.benchmark.com.main;

import com.benchmark.handledbactions.MngThreadsToHandleDBActions;
import com.benchmark.utils.Constants;

import java.util.Date;

/**
 * Created by shirela on 2/6/2017.
 */
public class Start {

        public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        System.out.println("Started :" + new Date());

        MngThreadsToHandleDBActions mngThreadsToHandleAction = new MngThreadsToHandleDBActions();
        mngThreadsToHandleAction.mngThreadsToHandleAction();
        long endTime   = System.currentTimeMillis();
        System.out.println("*************************************");
        System.out.println("Number of records - expected to insert: " + Constants.NUMNBER_OF_ITERATION_BULK * Constants.MAX_THREADS );
        System.out.println("Total time :" + (endTime-startTime)/1000 + " Seconds");
        System.out.println("Insert record per sec :" + (Constants.NUMNBER_OF_ITERATION_BULK * Constants.MAX_THREADS)/((endTime-startTime)/1000) + " Records/Sec");
        System.out.println("*************************************");
    }
}
