using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using ExternalSort;

namespace NaturalJoin
{
    public class Program
    {
        public static void Main(string[] args)
        {
            NaturalJoin join = new NaturalJoin();
            List<string> joinedFilenames = new List<string>();
            joinedFilenames.Add(JoinTwoFiles(join, 0, 0, 10, 20, 3, 3, 2, 2, 100, 200, 15));
            joinedFilenames.Add(JoinTwoFiles(join, 0, 0, 100, 100, 3, 3, 2, 2, 1000, 1000, 300));
            joinedFilenames.Add(JoinTwoFiles(join, 0, 0, 1000000, 1000000, 3, 3, 2, 2, 100000, 100000, 5000));

            Console.WriteLine("Joined files:");
            foreach (String filename in joinedFilenames)
            {
                Console.WriteLine(filename);
            }
            HelperFunctions.PressAnyKeyToContinue();
        }

        private static String JoinTwoFiles(NaturalJoin joinManager, 
            int file1MinValue, int file2MinValue, int file1MaxValue, int file2MaxValue, int file1FieldCount, int file2FieldCount,
            int file1FieldToMerge, int file2FieldToMerge, int file1RecordCount, int file2RecordCount, int recordsPerPage)
        {
            Console.WriteLine("Creating unsorted files...\n");
            string file1 = ExternalSort.HelperFunctions.CreateUnsortedFile(file1MinValue, file1MaxValue, file1FieldCount, file1RecordCount);
            string file2 = ExternalSort.HelperFunctions.CreateUnsortedFile(file2MinValue, file2MaxValue, file2FieldCount, file2RecordCount);
            
            joinManager.Initialize(file1, file2,file1FieldToMerge, file1FieldCount, file2FieldToMerge, file2FieldCount, recordsPerPage);
            String outputFilename = joinManager.Join();
            
            HelperFunctions.PressAnyKeyToContinue("Output file at {0} is a join between\n" + 
            "file1: min value = {1}, max value = {2}, {3} fields each record, {4} records total\n"
                + "file2: min value = {5}, max value = {6}, {7} fields each record, {8} records total", outputFilename, file1MinValue, file1MaxValue, file1FieldCount, file1RecordCount,
                file2MinValue, file2MaxValue, file2FieldCount, file2RecordCount);

            return outputFilename;
        }
    }
}