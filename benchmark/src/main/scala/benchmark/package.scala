

package object benchmark {

  case class SecEdgarCompanyInfo(lineNumber: String, companyName: String, companyCIKKey: String)

  case class Company(name: String,
                     domain: String,
                     yearFounded: String,
                     industry: String,
                     sizeRange: String,
                     locality: String,
                     country: String,
                     linkedinUrl: String,
                     currentEmployeeEstimate: String,
                     totalEmployeeEstimate: String)

}
