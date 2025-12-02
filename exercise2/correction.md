# Fundamentals of Data Engineering – Exercise 2 Correction
### Student: Javier Revuelta 

---

## Correction Notes

### Issues to solve:

#### (scrapper) Modify get_songs to use catalog (2 points)
- [ ] Points awarded: 2
- [ ] Comments:

#### (scrapper) Check logs for strange messages (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments: Good job here, good explanation.

#### (cleaner) Avoid processing catalogs (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Fix directory creation issue (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Additional validation rule (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments: Good validation rule

#### Code improvements (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments: Good detailed explanation and proposals. 

### Functionalities to add:

#### 'results' module (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### 'lyrics' module (2 points)
- [ ] Points awarded: 1.5
- [ ] Comments: Regex not always working to remove the chords, but overall, pretty good.

#### 'insights' module (2 points)
- [ ] Points awarded:  2
- [ ] Comments: Really good job using NLTK (although here is like killing flies with cannon balls). Also the json output is very convenien as it is easier to handle if this data is required by another application.

#### Main execution file (1 point)
- [ ] Points awarded: 1
- [ ] Comments: Very good.

---

## Total Score: 9.5 / 10 points

## General Comments:

Outstanding work, Javier! This is one of the best submissions in the class. You've demonstrated excellent understanding of data engineering principles and delivered a nearly flawless implementation.

**Exceptional Strengths:**

1. **Comprehensive Implementation**: Every single required component is implemented and functional
2. **Code Quality**: Well-documented, clean code with good explanations for decisions made
3. **Advanced Techniques**: Using NLTK for insights module shows initiative and proper tool selection (though slightly overkill for this use case - "killing flies with cannon balls" as noted)
4. **Output Format**: JSON output for insights is excellent - structured data that's easy to consume by other applications
5. **Problem Analysis**: Your log analysis and code improvement suggestions were detailed and well-reasoned

**Minor Areas for Enhancement:**

1. **Lyrics Module (1.5/2.0)**: The regex pattern doesn't catch all chord variations in every case. While it works for the vast majority of songs, edge cases exist. Consider using the existing `utils/chords.py` for a more robust solution.

2. **Logging**: While your pipeline works perfectly, adding logging would make it production-ready for real-world scenarios where monitoring and debugging are essential.

**What Made This Submission Excellent:**
- Complete functionality across all modules
- Proper use of subprocess library for orchestration
- Good validation rules that enhance the pipeline
- Thoughtful code improvements with detailed explanations
- No critical failures or architectural issues

**Minor Polish Suggestions:**
While some implementation choices could be slightly optimized (like the NLTK overhead), they don't detract from the quality of the work. In fact, knowing when to use libraries vs. custom solutions is a valuable skill, and your choice, while heavyweight, is technically sound and produces good results.

This is the level of work expected for professional data engineering. Keep up the excellent attention to detail and comprehensive approach. Well done!