package union

object Data {

  // Create the case classes for our domain
  case class Department(id: String, name: String)
  case class Employee(name: String, salary: Int)
  case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])
  val department1 = Department("123456", "Computer Science")
  val department2 = Department("789012", "Mechanical Engineering")
  val department3 = Department("345678", "Theater and Drama")
  val department4 = Department("901234", "Indoor Recreation")

  // Create the Employees
  val employee1 = Employee("michael", 100000)
  val employee2 = Employee("xiangrui", 120000)
  val employee3 = Employee("matei", 140000)
  val employee4 = Employee(null, 160000)
  val employee5 = Employee("michael", 80000)

  // Create the DepartmentWithEmployees instances from Departments and Employees
  val departmentWithEmployees1 = DepartmentWithEmployees(department1, Seq(employee1, employee2))
  val departmentWithEmployees2 = DepartmentWithEmployees(department2, Seq(employee3, employee4))
  val departmentWithEmployees3 = DepartmentWithEmployees(department3, Seq(employee5, employee4))
  val departmentWithEmployees4 = DepartmentWithEmployees(department4, Seq(employee2, employee3))

  val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
  val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
  val departmentsWithEmployeesSeq3 = Seq(employee1, employee2)
}
