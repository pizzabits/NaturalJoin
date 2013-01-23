using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using ExternalSort;

namespace NaturalJoin
{
    public class NaturalJoin
    {
        private const int GENERAL_RECORDS_PER_PAGE = 1000;

        private int _file1SizeInLines;
        private int _file2SizeInLines;
        private string _outputFilename;
        private StreamWriter _writer;
        private StreamReader _reader1;
        private StreamReader _reader2;
        private Page _page1;
        private Page _page2;
        private Page _outputPage;
        private int _recordsPerPage;
        private int _recordsIndexOutputPage;
        private int _fieldJoined1;
        private int _fieldJoined2;
        private int _fields1;
        private int _fields2;
        private List<Record> _currentFile1Records;
        private List<Record> _currentFile2Records;

        public void Initialize(string file1, string file2, int file1KeyField, int file1FieldCount, int file2KeyField, int file2FieldCount, int recordsPerPage)
        {
            _fieldJoined1 = file1KeyField;
            _fieldJoined2 = file2KeyField;
            _fields1 = file1FieldCount;
            _fields2 = file2FieldCount;
            _recordsPerPage = recordsPerPage < GENERAL_RECORDS_PER_PAGE ? recordsPerPage : GENERAL_RECORDS_PER_PAGE;

            // measure the input files
            _file1SizeInLines = File.ReadAllLines(file1).Length;
            _file2SizeInLines = File.ReadAllLines(file2).Length;

            // sort the input files using a random size of pages in the buffer pool
            Random r = new Random();
            Console.WriteLine("Sorting two files...\n");
            ExternalSort.ExternalSort externalSort = new ExternalSort.ExternalSort(r.Next(3, (int)Math.Ceiling(_file1SizeInLines / (double)_recordsPerPage)), _recordsPerPage);
            file1 = externalSort.Sort(file1, file1KeyField, file1FieldCount);
            externalSort = new ExternalSort.ExternalSort(r.Next(3, (int)Math.Ceiling(_file2SizeInLines / (double)_recordsPerPage)), _recordsPerPage);
            file2 = externalSort.Sort(file2, file2KeyField, file2FieldCount);

            // create an output file and a writer
            _outputFilename =  Path.GetTempFileName();
            _writer = new StreamWriter(_outputFilename);

            // start reading pages from the sorted input
            _reader1 = File.OpenText(file1);
            _page1 = new Page(file1KeyField, HelperFunctions.ReadPage(_reader1, _recordsPerPage));
            _reader2 = File.OpenText(file2);
            _page2 = new Page(file2KeyField, HelperFunctions.ReadPage(_reader2, _recordsPerPage));

            // prepare an output page consisting of joined fields
            _outputPage = new Page(file1FieldCount + file2FieldCount - 1, _recordsPerPage);
            _currentFile1Records = new List<Record>();
            _currentFile2Records = new List<Record>();

            ExternalSort.HelperFunctions.PressAnyKeyToContinue("Created and sorted two files in {0} and {1}", file1, file2);
        }

        /// <summary>
        /// Joins two sorted files by their joining fields.
        /// </summary>
        public String Join()
        {
            bool finished = false;
            int records1Index = 0;
            int records2Index = 0;

            while (!finished)
            {
                if (_page1.Records == null || _page2.Records == null)
                {
                    finished = true;
                    break;
                }

                // iterate over the records of the two files to find equality between record's keys.
                while (records1Index < _page1.Records.Length && _page1.Records[records1Index] != null 
                    && records2Index < _page2.Records.Length && _page2.Records[records2Index] != null)
                {
                    // file1's record < file2's record
                    if (_page1.Records[records1Index][_fieldJoined1] < _page2.Records[records2Index][_fieldJoined2])
                    {
                        if (_currentFile1Records.Count > 0 && _currentFile2Records.Count > 0)
                        {
                            // although the current records's keys aren't equal, the smaller key may be related
                            // to the previous key that hasn't yet been joined.
                            // add more records with the same key as the saved records, if exist
                            if (_page1.Records[records1Index][_fieldJoined1] == _currentFile1Records.First()[_fieldJoined1])
                            {
                                _currentFile1Records.Add(_page1.Records[records1Index]);
                            }
                            else
                            {
                                // the current keys are different and the record's lists contain records to join
                                ProduceJoinedRecords();
                            }
                        }
                        records1Index++;
                        continue;
                    }
                    // file1's record = file2's record
                    if (_page1.Records[records1Index][_fieldJoined1] == _page2.Records[records2Index][_fieldJoined2])
                    {
                        if (_currentFile1Records.Count > 0 && _currentFile2Records.Count > 0 &&
                            _page1.Records[records1Index][_fieldJoined1] != _currentFile1Records.First()[_fieldJoined1])
                        {
                            // the current keys are equal to eachother but different than the record's join lists
                            ProduceJoinedRecords();
                        }
                        // now that the previous key has been joined and the lists are empty, add the current records
                        _currentFile1Records.Add(_page1.Records[records1Index]);
                        _currentFile2Records.Add(_page2.Records[records2Index]);
                        records1Index++;
                        records2Index++;
                        continue;
                    }
                    // file1's record > file2's record
                    if (_page1.Records[records1Index][_fieldJoined1] > _page2.Records[records2Index][_fieldJoined2])
                    {
                        if (_currentFile1Records.Count > 0 && _currentFile2Records.Count > 0)
                        {
                            // although the current records's keys aren't equal, the smaller key may be related
                            // to the previous key that hasn't yet been joined.
                            // add more records with the same key as the saved records, if exist
                            if (_page2.Records[records2Index][_fieldJoined2] == _currentFile2Records.First()[_fieldJoined2])
                            {
                                _currentFile2Records.Add(_page2.Records[records2Index]);
                            }
                            else
                            {
                                // the current keys are different and the record's lists contain records to join
                                ProduceJoinedRecords();
                            }
                        }
                        records2Index++;
                        continue;
                    }
                }

                // if one of the pages exceeded, load another one
                if (records1Index == _page1.Records.Length || _page1.Records[records1Index] == null)
                {
                    _page1 = new Page(_fieldJoined1, HelperFunctions.ReadPage(_reader1, _recordsPerPage));
                    if (_page1.Records == null)
                        break;
                    records1Index = 0;
                }
                if (records2Index == _page2.Records.Length || _page2.Records[records2Index] == null)
                {
                    _page2 = new Page(_fieldJoined2, HelperFunctions.ReadPage(_reader2, _recordsPerPage));
                    if (_page2.Records == null)
                        break;
                    records2Index = 0;
                }
            }

            // if there were more records to join after finished reading from the files, join them
            if (_currentFile1Records.Count > 0 && _currentFile2Records.Count > 0)
                ProduceJoinedRecords();
            // write the last page if exists
            if (_outputPage.Records.First() != null)
            {
                _outputPage.WritePage(_writer);
            }
            _writer.Close();
            return _outputFilename;
        }

        /// <summary>
        /// Joins two lists of records who have the same key as their joining field.
        /// The lists are related to two different files.
        /// The method produces records consisting of both record's values and only one search-field [which is equals in both records],
        /// then adds them to the current output page and purges the lists as an initialization for the next key.
        /// The number of records in the page stays the same as the number of records in the input file,
        /// although each record will probably be bigger since its a join of two records.
        /// </summary>
        private void ProduceJoinedRecords()
        {
            foreach (Record item1 in _currentFile1Records)
            {
                foreach (Record item2 in _currentFile2Records)
                {
                    int[] joinedFields = new int[_fields1 + _fields2 - 1];
                    // from file1's record, take all fields
                    for (int i = 0; i < _fields1; i++)
                    {
                        joinedFields[i] = item1[i];
                    }
                    int nextField = _fields1;
                    // from file2's record, take the fields without the joined field
                    for (int i = 0; i < _fields2; i++)
                    {
                        if (i == _fieldJoined2) // already added
                            continue;
                        joinedFields[nextField++] = item2[i];
                    }
                    _outputPage.Records[_recordsIndexOutputPage++] = new Record(0, joinedFields);  ///// TODO: ADD CTOR WITHOUT SPECIAL FIELD!!!
                    if (_recordsIndexOutputPage == _recordsPerPage)
                    {
                        _outputPage.WritePage(_writer);
                        _outputPage = new Page(_fields1 + _fields2 - 1, _recordsPerPage);
                        _recordsIndexOutputPage = 0;
                    }
                }
            }
            _currentFile1Records.Clear();
            _currentFile2Records.Clear();
        }
    }
}