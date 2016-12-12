// Out-of-pocket cost calculator as defined in LMI methodology paper v28 and corresponding Excel sheet

// TODO: optim: improve included, outside personIndex check gives 10ms, can we skip dead space altogether in loops?
// TODO: optim: consolidate three-stage loops?
// TODO: see bug! comment

package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
)

const (
	// utilization multiplier (UtilIndex)
	utilizationMultiplier = 1.0

	// inflation, reflect discount off allowable cost in utilization table (ACMult)
	inflationMultiplier = 1.138
)

type OopcRepository interface {
	GetOopcUtilization(ctx context.Context, marketYear int, key string) (*OopcUtilization, error)
	GetOopcUtilizationByQuery(ctx context.Context, marketYear int, key string) (*OopcUtilization, error)
	MustLoadCache()
}

type OopcHousehold struct {
	People []*OopcPerson `json:"people"`

	// enable for debug statements
	Debug bool

	// stash for convenience
	plan     *Plan
	variant  CSR
	oopcRepo OopcRepository

	// intermediaries
	sharedServiceMap       []*sharedServiceItems
	utilGroups             []*utilizationGroup
	maxCoinsSpecialtyDrugs float64
	maxInpatientCopayDays  float64
}

type OopcPerson struct {
	Age              int              `json:"age"`
	Gender           Gender           `json:"gender"`
	UtilizationLevel UtilizationLevel `json:"utilization_level"`

	services   []*serviceType
	ukeyPrefix string
}

type OopcUtilization struct {
	Key           string
	AgeGroup      string
	Gender        string
	UtilLevel     string
	NoObs         uint // number of observed
	Service       string
	UnitCost      float64
	Utilization   float64
	Units         string
	AnnualCost    float64
	ExtendedCost  float64
	ProvVisits    float64
	LabTests      float64
	Scripts       float64
	HospDays      float64
	Other         float64
	OtherDollars  float64
	ExtendedUnits float64
}

type UtilizationLevel float64

const (
	LowUtil    UtilizationLevel = 3
	MediumUtil                  = 5.5
	HighUtil                    = 10
)

var UtilizationLevelDeciles = map[UtilizationLevel]string{
	LowUtil:    "Decile 3",
	MediumUtil: "Decile 5.5",
	HighUtil:   "Decile 10",
}

var UtilizationLevelValues = map[UtilizationLevel]string{
	LowUtil:    "Low",
	MediumUtil: "Medium",
	HighUtil:   "High",
}

func (ul UtilizationLevel) Decile(marketYear int, age int, gender Gender) string {
	// For 2017+, 18-25M have decile 4 instead of decile 3
	if marketYear > 2016 && ul == LowUtil && age > 17 && age < 26 && gender == Male {
		return "Decile 4"
	}

	v, ok := UtilizationLevelDeciles[ul]
	if !ok {
		return "unknown"
	}
	return v
}

func (ul UtilizationLevel) String() string {
	v, ok := UtilizationLevelValues[ul]
	if !ok {
		return "unknown"
	}
	return v
}

func (ul *UtilizationLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("UtilizationLevel should be a string, got %s", data)
	}
	for k, v := range UtilizationLevelValues {
		if v == s {
			*ul = k
			return nil
		}
	}
	return fmt.Errorf("invalid UtilizationLevel %q", s)
}

func (ul *UtilizationLevel) Scan(s string) error {
	for k, v := range UtilizationLevelValues {
		if v == s {
			*ul = k
			return nil
		}
	}
	return fmt.Errorf("invalid UtilizationLevel %q", s)
}

func (ul UtilizationLevel) MarshalJSON() ([]byte, error) {
	return json.Marshal(ul.String())
}

type serviceType struct {
	benefit     *Benefit
	costSharing *CostSharing
	utilization *OopcUtilization // TODO: rename utilizationData or utilData, observed vs calculated
	rule        *csRule
	service     oopcService

	unitCost    float64
	util        float64
	copay       float64
	coinsurance float64 // TODO: coinsurance"Rate" naming is consistent with plan variables
	csRate      float64

	totalClaimCost float64 // CCTC(i,j)
	deductiblePct  float64

	totalSpendingAmt      float64
	benPaymentsDed        float64 // beneficiary payments deductible
	benPaymentsCS         float64 // beneficiary payments cost sharing
	planPaymentsCSPhase   float64
	planPaymentsMoopPhase float64

	spendStatus      oopcStatus
	spendStatusDedF  float64 // cache boolean as 0/1 floats
	spendStatusMOOPF float64
	spendStatusCSF   float64
}

type sharedServiceItems struct {
	benefit     *Benefit
	costSharing *CostSharing
	rule        *csRule
}

type oopcService int

const (
	ip_fac oopcService = iota
	ip_prof
	op_ambulance
	op_chiro
	op_ct_mri
	op_diagnostic_cardiology
	op_dialysis
	op_dme
	op_er_fac
	op_eye
	op_jcode
	op_lab
	op_mh
	op_op_fac
	op_other
	op_prev
	op_primcare
	op_rehab
	op_speccare
	op_surg
	op_ultrasound
	op_xray
	rx_generic
	rx_npbrand
	rx_pbrand
	rx_specialty
)

var oopcServiceValues = [...]string{
	"ip_fac",
	"ip_prof",
	"op_ambulance",
	"op_chiro",
	"op_ct_mri",
	"op_diagnostic_cardiology",
	"op_dialysis",
	"op_dme",
	"op_er_fac",
	"op_eye",
	"op_jcode",
	"op_lab",
	"op_mh",
	"op_op_fac",
	"op_other",
	"op_prev",
	"op_primcare",
	"op_rehab",
	"op_speccare",
	"op_surg",
	"op_ultrasound",
	"op_xray",
	"rx_generic",
	"rx_npbrand",
	"rx_pbrand",
	"rx_specialty",
}

func (os oopcService) String() string {
	if int(os) < len(oopcServiceValues) {
		return oopcServiceValues[os]
	}
	return "unknown"
}

// OOPC services to CMS plan benefit labels
var OopcServices = map[oopcService]string{
	ip_fac:                   "Inpatient Hospital Services (e.g., Hospital Stay)",
	ip_prof:                  "Inpatient Physician and Surgical Services",
	op_ambulance:             "Emergency Transportation/Ambulance",
	op_chiro:                 "Chiropractic Care",
	op_ct_mri:                "Imaging (CT/PET Scans, MRIs)",
	op_diagnostic_cardiology: "X-rays and Diagnostic Imaging",
	op_dialysis:              "Dialysis",
	op_dme:                   "Durable Medical Equipment",
	op_er_fac:                "Emergency Room Services",
	op_eye:                   "Routine Eye Exam (Adult)",
	op_jcode:                 "Chemotherapy",
	op_lab:                   "Laboratory Outpatient and Professional Services",
	op_mh:                    "Mental/Behavioral Health Outpatient Services",
	op_op_fac:                "Outpatient Facility Fee (e.g.,  Ambulatory Surgery Center)",
	op_other:                 "", // custom logic
	op_prev:                  "Preventive Care/Screening/Immunization",
	op_primcare:              "Primary Care Visit to Treat an Injury or Illness",
	op_rehab:                 "Outpatient Rehabilitation Services",
	op_speccare:              "Specialist Visit",
	op_surg:                  "Outpatient Surgery Physician/Surgical Services",
	op_ultrasound:            "X-rays and Diagnostic Imaging",
	op_xray:                  "X-rays and Diagnostic Imaging",
	rx_generic:               "Generic Drugs",
	rx_npbrand:               "Non-Preferred Brand Drugs",
	rx_pbrand:                "Preferred Brand Drugs",
	rx_specialty:             "Specialty Drugs",
}

var sortedServices []oopcService

type OopcServiceSlice []oopcService

func (p OopcServiceSlice) Len() int           { return len(p) }
func (p OopcServiceSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p OopcServiceSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func init() {
	for k := range OopcServices {
		sortedServices = append(sortedServices, k)
	}
	sort.Sort(OopcServiceSlice(sortedServices))
}

func (hh *OopcHousehold) Clone() *OopcHousehold {
	newhh := &OopcHousehold{}
	for _, e := range hh.People {
		newhh.People = append(newhh.People, &OopcPerson{Age: e.Age, Gender: e.Gender, UtilizationLevel: e.UtilizationLevel})
	}
	return newhh
}

func NewOOPCHousehold(plainHH *Household) *OopcHousehold {
	plainCoverageHH := plainHH.CoverageFamily()
	oopcHH := OopcHousehold{}
	for i := range plainCoverageHH.People {
		p := &plainCoverageHH.People[i]
		oopcHH.People = append(oopcHH.People, &OopcPerson{Age: p.Age, Gender: p.Gender, UtilizationLevel: p.UtilizationLevel})
	}
	return &oopcHH
}

func OOPCFromPlainHousehold(ctx context.Context, marketYear int, plainHH *Household, plan *Plan, variant CSR, oopcRepo OopcRepository) (oopc float64, e error) {
	oopcHH := NewOOPCHousehold(plainHH)
	return oopcHH.OOPC(ctx, marketYear, plan, variant, oopcRepo)
}

func (hh *OopcHousehold) OOPC(ctx context.Context, marketYear int, plan *Plan, variant CSR, oopcRepo OopcRepository) (oopc float64, e error) {
	if (variant.SilverVariant() && plan.MetalLevel != Silver) || (variant.TribalVariant() && plan.MetalLevel == Catastrophic) {
		variant = NoCSR
	}

	// Loads the correct variant information
	plan.SetVariant(variant)

	hh.plan = plan
	hh.variant = variant
	hh.oopcRepo = oopcRepo

	if err := hh.buildSharedServiceMap(); err != nil {
		return 0, err
	}

	if err := hh.initVars(ctx, marketYear); err != nil {
		return 0, err
	}

	if err := hh.populateUtilizationGroups(); err != nil {
		return 0, err
	}

	if hh.Debug {
		hh.dumpModelInputs()
	}

	if err := hh.runCalculation(); err != nil {
		return 0, err
	}

	// tally cost across people-services
	for _, p := range hh.People {
		for _, s := range p.services {
			oopc += s.benPaymentsDed + s.benPaymentsCS
		}
	}

	oopc = roundToPlace(oopc, 2)

	return
}

// initialize person-benefit category cost and cost sharing rates (Excel step 1)
func (hh *OopcHousehold) initVars(ctx context.Context, marketYear int) error {
	totalAllowedCostDefined := 0.0
	totalCSDefined := 0.0
	totalCSRate := 0.0 // total cost sharing rate for all benefits combined

	if hh.plan.MaxCoinsSpecialtyDrugs == -1 {
		hh.maxCoinsSpecialtyDrugs = 1e6
	} else {
		hh.maxCoinsSpecialtyDrugs = hh.plan.MaxCoinsSpecialtyDrugs
	}

	if hh.plan.MaxInpatientCopayDays == -1 {
		hh.maxInpatientCopayDays = 1e5
	} else {
		hh.maxInpatientCopayDays = float64(hh.plan.MaxInpatientCopayDays)
	}

	for _, p := range hh.People {
		p.ukeyPrefix = ageGroup(marketYear, p.Age) + p.Gender.ShortString() + p.UtilizationLevel.Decile(marketYear, p.Age, p.Gender)

		// used below for inpatient facility / inpatient professional beneft logic
		ipProfUtilization, err := hh.oopcRepo.GetOopcUtilization(ctx, marketYear, p.ukeyPrefix+"ip_prof")
		if err != nil {
			return err
		}

		p.services = make([]*serviceType, len(sortedServices))

		for _, service := range sortedServices {
			s, err := hh.addServiceType(ctx, marketYear, p, service)
			if err != nil {
				return err
			}

			//
			// initialize person-service values
			//
			if marketYear == 2016 {
				s.unitCost = inflationMultiplier * s.utilization.UnitCost
			} else {
				s.unitCost = s.utilization.UnitCost
			}

			s.util = utilizationMultiplier * s.utilization.Utilization

			// for primary care, adjust utilization for a plan's free visits
			if service == op_primcare && hh.plan.FreePcVisits > 0 {
				var adjustment float64
				if s.util > float64(hh.plan.FreePcVisits) {
					adjustment = float64(hh.plan.FreePcVisits)
				} else {
					adjustment = s.util
				}

				s.util -= adjustment
			}

			s.totalClaimCost = s.unitCost * s.util
			s.copay = s.effectiveCopayAmt()
			s.coinsurance = s.effectiveCoinsuranceRate()

			// for inpatient facility, adjust copay if per stay rather than per day
			if service == ip_fac && strings.Contains(strings.ToLower(s.costSharing.CopayOptions), "per stay") {
				if s.util > 0 {
					// stays per day
					s.copay *= ipProfUtilization.Utilization / s.util
				} else {
					s.copay = 0
				}
			}

			// for specialty drug coinsurance, clamp coinsurance to maximum
			if service == rx_specialty && s.unitCost > 0 {
				s.coinsurance = minFloat64(s.coinsurance, hh.maxCoinsSpecialtyDrugs/s.unitCost)
			}

			if s.unitCost > 0 {
				s.csRate = (s.copay + s.coinsurance*s.unitCost) / s.unitCost
			}

			// inpatient facility adjustment if utilization is larger than maximum copay days
			if service == ip_fac && float64(hh.maxInpatientCopayDays) < s.util && strings.Contains(strings.ToLower(s.costSharing.CopayOptions), "per day") && s.unitCost*s.util > 0 {
				s.csRate -= (s.util - float64(hh.maxInpatientCopayDays)) * s.copay / (s.unitCost * s.util)
			}

			s.csRate = minFloat64(s.csRate, 1.0)

			if service != op_other {
				totalAllowedCostDefined += s.totalClaimCost
				totalCSDefined += s.csRate * s.totalClaimCost
			}
		}
	}

	if totalAllowedCostDefined != 0 {
		totalCSRate = totalCSDefined / totalAllowedCostDefined
	}

	for _, p := range hh.People {
		for _, s := range p.services {
			// for op_other -- with no underlying cost coverage -- we use the average of all other plan service cost sharing
			if s.service == op_other {
				s.csRate = totalCSRate
			}

			// set percentage applied to deductible
			if s.rule.beforeDeductible == false { // after
				s.deductiblePct = 1.0
			} else {
				s.deductiblePct = 1.0 - s.csRate
			}
		}
	}

	if hh.Debug {
		log.Printf("totalCSRate: %f", totalCSRate)
	}

	return nil
}

// run calculation (Excel step 3)
func (hh *OopcHousehold) runCalculation() error {
	var spendLevel float64
	var iteration int

	for spendLevel < 1 {
		if iteration > 0 {
			// step 3a: set spend level
			minNextPct := 1.0

			for _, ug := range hh.utilGroups {
				if roundFloat64(ug.nextApplicablePct, 8) > roundFloat64(spendLevel, 8) && ug.nextApplicablePct < minNextPct {
					minNextPct = ug.nextApplicablePct
				}
			}
			if hh.Debug {
				log.Printf("3a minNextPct %f", minNextPct)
			}
			spendLevel = minNextPct

			// step 3b: set spending amount for each person-benefit(service) category
			for pi, p := range hh.People {
				for si, s := range p.services {
					previous := s.totalSpendingAmt
					s.totalSpendingAmt = spendLevel * s.totalClaimCost
					totalSpendingChange := s.totalSpendingAmt - previous

					switch s.spendStatus {
					case oopcDed:
						s.benPaymentsDed += s.deductiblePct * totalSpendingChange
						s.benPaymentsCS += (1.0 - s.deductiblePct) * totalSpendingChange
					case oopcCS:
						s.benPaymentsCS += s.csRate * totalSpendingChange
						s.planPaymentsCSPhase += (1.0 - s.csRate) * totalSpendingChange
					case oopcMOOP:
						s.planPaymentsMoopPhase += totalSpendingChange
					}

					if hh.Debug {
						log.Printf("3b ei %d si %2d %25s benPayments Ded %10.6f CS %10.6f -- planPayments CSPhase %10.6f MoopPhase %10.6f",
							pi, si, s.service, s.benPaymentsDed, s.benPaymentsCS, s.planPaymentsCSPhase, s.planPaymentsMoopPhase)
					}
				}
			}

			for ugi, ug := range hh.utilGroups {
				var totalDeductible, totalOOP float64

				for pi, p := range hh.People {
					for _, s := range p.services {
						if !ug.planLevel && ug.personIndex != pi { // repeated from included() for perf
							continue
						}
						if s.included(ug, pi) {
							totalDeductible += s.benPaymentsDed
							totalOOP += s.benPaymentsDed + s.benPaymentsCS
						}
					}
				}

				ug.remainingDeductible = roundFloat64(ug.applicableDeductible-totalDeductible, 4)
				ug.remainingMoop = roundFloat64(ug.applicableMoop-totalOOP, 4)

				if hh.Debug {
					log.Printf("3b ugi %d remainingDeductible %f remainingMoop %f", ugi, ug.remainingDeductible, ug.remainingMoop)
				}
			}
		}

		// step 3c: update status for each utilization group
		for ugi, ug := range hh.utilGroups {
			if ug.dedMoop == dmMOOP && ug.remainingMoop <= 0 {
				ug.status = oopcMOOP
			} else if ug.dedMoop == dmDeductible && ug.remainingDeductible > 0 {
				ug.status = oopcDed
			} else if ug.dedMoop == dmMOOP {
				ug.status = oopcCSNoDed
			} else {
				ug.status = oopcCS
			}
			if hh.Debug {
				log.Printf("3c ugi %2d status %d", ugi, ug.status)
			}
		}

		// step 3d: update person-service category status
		for pi, p := range hh.People {
			for _, s := range p.services {
				var maxSpendStatus uint
				for _, ug := range hh.utilGroups {
					if !ug.planLevel && ug.personIndex != pi { // repeated from included() for perf
						continue
					}
					if s.included(ug, pi) {
						if maxSpendStatus < uint(ug.status) {
							maxSpendStatus = uint(ug.status)
						}
					}
				}

				s.spendStatusDedF, s.spendStatusMOOPF, s.spendStatusCSF = 0, 0, 0

				switch maxSpendStatus {
				case 1:
					s.spendStatus = oopcDed
					s.spendStatusDedF = 1
				case 3:
					s.spendStatus = oopcMOOP
					s.spendStatusMOOPF = 1
				default:
					s.spendStatus = oopcCS
					s.spendStatusCSF = 1
				}
			}
		}

		// step 3e: determine spending rate, and applicable spending percent, of next iteration
		for ugi, ug := range hh.utilGroups {
			var deductibleSpendRate, csSpendRate, moopSpendRate float64

			for pi, p := range hh.People {
				for _, s := range p.services {
					if !ug.planLevel && ug.personIndex != pi { // repeated from included() for perf
						continue
					}
					if s.included(ug, pi) {
						deductibleSpendRate += s.spendStatusDedF * s.totalClaimCost * s.deductiblePct
						csSpendRate += s.spendStatusCSF*s.totalClaimCost*s.csRate + s.spendStatusDedF*s.totalClaimCost*(1-s.deductiblePct)
						moopSpendRate += s.spendStatusMOOPF * s.totalClaimCost
					}
				}
			}

			var nextSpendPctDeductible float64 = 1.0
			var nextSpendPctCS float64 = 1.0

			// deductible condition met?
			if deductibleSpendRate > 0 && ug.remainingDeductible > 0 && ug.status == oopcDed {
				nextSpendPctDeductible = spendLevel + ug.remainingDeductible/deductibleSpendRate
			}

			// CS condition met?
			if csSpendRate >= 0 && deductibleSpendRate >= 0 && csSpendRate+deductibleSpendRate > 0 && ug.remainingMoop > 0 {
				if ug.status == oopcCSNoDed || ug.status == oopcCS {
					nextSpendPctCS = spendLevel + ug.remainingMoop/(csSpendRate+deductibleSpendRate)
				}
			}

			ug.nextApplicablePct = minFloat64(nextSpendPctDeductible, nextSpendPctCS)

			if hh.Debug {
				log.Printf("3e ugi %2d dedRate %9.3f csRate %9.3f moopRate %9.3f nextSpendPctDeductible %5.3f nextSpendPctCS %5.3f", ugi, deductibleSpendRate, csSpendRate, moopSpendRate, nextSpendPctDeductible, nextSpendPctCS)
			}
		}

		iteration++

		if iteration > 100 {
			return errors.New("calculation has not converged in 100 iterations")
		}

		if hh.Debug {
			log.Printf("loop spendLevel %f", spendLevel)
		}
	}

	if hh.Debug {
		log.Printf("final spendLevel %f iterations %d", spendLevel, iteration)
	}

	return nil
}

func (hh *OopcHousehold) addServiceType(ctx context.Context, marketYear int, p *OopcPerson, service oopcService) (*serviceType, error) {
	sharedItems := hh.sharedServiceMap[service]

	// load observed utilization data
	utilization, err := hh.oopcRepo.GetOopcUtilization(ctx, marketYear, p.ukeyPrefix+oopcServiceValues[service])
	if err != nil {
		return nil, err
	}

	s := &serviceType{service: service, benefit: sharedItems.benefit, costSharing: sharedItems.costSharing, utilization: utilization, rule: sharedItems.rule}
	p.services[service] = s

	return s, nil
}

func (hh *OopcHousehold) buildSharedServiceMap() error {
	var benefit *Benefit
	var costSharing *CostSharing
	var rule *csRule

	hh.sharedServiceMap = make([]*sharedServiceItems, len(oopcServiceValues))

	for _, service := range sortedServices {
		if service == op_other {
			benefit = new(Benefit)
			costSharing = new(CostSharing)
			rule = &csRule{subjDeductible: true, subjMoop: true, beforeDeductible: false}
		} else {
			// find benefit
			benefitName := OopcServices[service]
			for i := range hh.plan.Benefits {
				b := &hh.plan.Benefits[i]
				if b.Name == benefitName {
					benefit = b
				}
			}

			if benefit == nil {
				if hh.Debug {
					log.Printf("could not find benefit in plan [%s] corresponding to service [%s]", hh.plan.ID, service)
				}
				benefit = new(Benefit)
			}

			// find cost coverage
			for i := range benefit.CostSharings {
				cc := &benefit.CostSharings[i]
				if cc.CSR == hh.variant && cc.NetworkTier == InNetwork {
					costSharing = cc
				}
			}

			if costSharing == nil {
				if hh.Debug {
					log.Printf("expected to find InNetwork cost sharing for plan [%s] and benefit [%s]", hh.plan.ID, benefit.Name)
				}
				costSharing = new(CostSharing)
			}

			// populate copay / coinsurance rules
			if !benefit.Covered || costSharing == nil { // TODO: bug!
				rule = &csRule{}
			} else {
				_rule, ok := csRulesMap[costSharing.CopayOptions+".."+costSharing.CoinsuranceOptions]
				if !ok {
					return fmt.Errorf("failed to find cost coverage opts [%s // %s]", costSharing.CopayOptions, costSharing.CoinsuranceOptions)
				}
				_rule.subjMoop = !benefit.IsExcludedFromMOOP
				rule = &_rule
			}
		}

		hh.sharedServiceMap[service] = &sharedServiceItems{benefit: benefit, costSharing: costSharing, rule: rule}
	}

	return nil
}

func (hh *OopcHousehold) dumpModelInputs() {
	log.Printf("---------------- Model Inputs -----------------------")

	log.Printf("plan level cost sharing:")
	// TODO: print deductible/moop inputs
	log.Printf("    primary care CS after this many visits: %d", hh.plan.FreePcVisits)
	log.Printf("    max coins specialty drug: %f", hh.plan.MaxCoinsSpecialtyDrugs)
	log.Printf("    max inpatient copay days: %d", hh.plan.MaxInpatientCopayDays)

	log.Printf("service level cost sharing:")
	for _, service := range sortedServices {
		for _, s := range hh.People[0].services {
			if s.service == service {
				yesno := func(b bool) string {
					if b {
						return "yes"
					} else {
						return "no"
					}
				}
				beforeafter := func(b bool) string {
					if b {
						return "before"
					} else {
						return "after"
					}
				}

				log.Printf("%25s  copay %3.f  coins %0.2f  subjDed %3s  subjMOOP %3s  apply before/after %s",
					s.service, s.effectiveCopayAmt(), s.effectiveCoinsuranceRate(),
					yesno(s.rule.subjDeductible), yesno(s.rule.subjMoop),
					beforeafter(s.rule.beforeDeductible))
			}
		}
	}

	log.Printf("utilization groups:")
	for _, ug := range hh.utilGroups {
		log.Printf("    %10s + %8s  ded %5d  moop %5d", ug.dedMoop, ug.medicalRx, int(ug.applicableDeductible), int(ug.applicableMoop))
	}

	// TODO: setup vars

	log.Printf("inclusion matrix:")
	for _, service := range sortedServices {
		var buf string
		for _, s := range hh.People[0].services {
			if s.service == service {
				buf += fmt.Sprintf("  %25s  ", service)
				for _, ug := range hh.utilGroups {
					if s.included(ug, 0) {
						buf += "1 "
					} else {
						buf += "0 "
					}
				}
			}
		}
		log.Println(buf)
	}

	log.Printf("initialization:")
	for pi, p := range hh.People {
		for _, service := range sortedServices {
			for _, s := range p.services {
				if s.service == service {
					log.Printf("  ei %d %25s: unitCost %8.3f util %8.3f totalClaimCost %8.3f copay %8.3f coinsurance %8.3f csRate %8.3f dpct %8.3f",
						pi, service, s.unitCost, s.util, s.totalClaimCost, s.copay, s.coinsurance, s.csRate, s.deductiblePct)
				}
			}
		}
	}

	log.Printf("------------- end Model Inputs ---------------------")
}

type csRule struct {
	copay            bool // false == 0, true == plan value
	coinsurance      bool // false == 100%, true == plan value
	subjDeductible   bool // subject to deductible?
	subjMoop         bool // subject to MOOP?
	beforeDeductible bool // applies before deductible (or after)
}

// cf. Copay & Coinsurance extrapolation table, P&B Data Write-Up
// lookup on concat of cost sharing strings
// TODO: make these enums in Plan
var csRulesMap = map[string]csRule{
	"No Charge..No Charge":                                     {false, false, false, true, false},
	"No Charge..Not Applicable":                                {false, false, false, true, false},
	"Not Applicable..No Charge":                                {false, false, false, true, false},
	"Not Applicable..Not Applicable":                           {false, false, false, true, false},
	"No Charge..No Charge after deductible":                    {false, false, true, true, false},
	"No Charge after deductible..No Charge":                    {false, false, true, true, false},
	"No Charge after deductible..No Charge after deductible":   {false, false, true, true, false},
	"No Charge after deductible..Not Applicable":               {false, false, true, true, false},
	"Not Applicable..No Charge after deductible":               {false, false, true, true, false},
	"No Charge..":                                              {false, true, false, true, false},
	"Not Applicable..":                                         {false, true, false, true, false},
	"Copay before deductible..":                                {false, true, true, true, false},
	"Copay before deductible..Coinsurance after deductible":    {false, true, true, true, false},
	"No Charge..Coinsurance after deductible":                  {false, true, true, true, false},
	"No Charge after deductible..Coinsurance after deductible": {false, true, true, true, false},
	"No Charge after deductible..":                             {false, true, true, true, false},
	"Not Applicable..Coinsurance after deductible":             {false, true, true, true, false},
	"..No Charge":                                              {true, false, false, true, false},
	"..Not Applicable":                                         {true, false, false, true, false},
	"Copay after deductible..No Charge":                        {true, false, true, true, false},
	"Copay after deductible..No Charge after deductible":       {true, false, true, true, false},
	"Copay after deductible..Not Applicable":                   {true, false, true, true, false},
	"..No Charge after deductible":                             {true, false, true, true, false},
	"Copay before deductible..No Charge":                       {true, false, true, true, true},
	"Copay before deductible..No Charge after deductible":      {true, false, true, true, true},
	"Copay before deductible..Not Applicable":                  {true, false, true, true, true},
	"..": {true, true, false, true, false},
	"Copay after deductible..Coinsurance after deductible":           {true, true, true, true, false},
	"Copay after deductible..":                                       {true, true, true, true, false},
	"..Coinsurance after deductible":                                 {true, true, true, true, false},
	"Copay per Day..":                                                {true, true, false, true, false},
	"Copay per Day..No Charge":                                       {true, false, false, true, false},
	"Copay per Day..Not Applicable":                                  {true, false, false, true, false},
	"Copay per Day..Coinsurance after deductible":                    {true, true, true, true, false},
	"Copay per Day..No Charge after deductible":                      {true, false, true, true, false},
	"Copay per Day after deductible..No Charge after deductible":     {true, false, true, true, false},
	"Copay per Day after deductible..Not Applicable":                 {true, false, true, true, false},
	"Copay per Day after deductible..Coinsurance after deductible":   {true, true, true, true, false},
	"Copay per Stay..":                                               {true, false, true, true, false},
	"Copay per Stay..No Charge":                                      {true, false, false, true, false},
	"Copay per Stay..Not Applicable":                                 {true, false, false, true, false},
	"Copay per Stay..No Charge after deductible":                     {true, false, true, true, false},
	"Copay per Stay..Coinsurance after deductible":                   {true, true, true, true, false},
	"Copay per Stay after deductible..No Charge":                     {true, false, true, true, false},
	"Copay per Stay after deductible..No Charge after deductible":    {true, false, true, true, false},
	"Copay per Stay after deductible..Not Applicable":                {true, false, true, true, false},
	"Copay per Stay before deductible..No Charge after deductible":   {true, false, true, true, true},
	"Copay per Stay before deductible..Not Applicable":               {true, false, true, true, true},
	"Copay per Stay after deductible..Coinsurance after deductible":  {true, true, true, true, false},
	"Copay per Stay before deductible..Coinsurance after deductible": {true, true, true, true, false},
	"Copay per Day before deductible..Coinsurance after deductible":  {false, true, true, true, false},
	"Copay per Day before deductible..No Charge after deductible":    {true, false, true, true, true},
	"Copay per Day before deductible..Not Applicable":                {true, false, true, true, false},
}

func (s *serviceType) effectiveCopayAmt() float64 {
	if !s.benefit.Covered || !s.rule.copay {
		return 0
	}

	return s.costSharing.CopayAmount
}

func (s *serviceType) effectiveCoinsuranceRate() float64 {
	if s.service == op_other {
		return 0
	}

	if !s.benefit.Covered {
		return 1.0 // 100%
	} else if !s.rule.coinsurance {
		return 0
	}

	return s.costSharing.CoinsuranceRate
}

func (s *serviceType) included(ug *utilizationGroup, personIndex int) (result bool) {
	if !ug.planLevel && ug.personIndex != personIndex {
		return false
	}

	if ug.medicalRx == mrxMedical && strings.HasPrefix(oopcServiceValues[s.service], "rx_") {
		return false
	}
	if ug.medicalRx == mrxDrug && !strings.HasPrefix(oopcServiceValues[s.service], "rx_") {
		return false
	}

	if ug.dedMoop == dmDeductible {
		return s.rule.subjDeductible
	} else {
		return s.rule.subjMoop
	}
}

// TODO: revisit naming of these internal types, pet-wide, might push into oopc package

type dedMoopType int

const (
	_ dedMoopType = iota
	dmDeductible
	dmMOOP
)

var dedMoopTypeValues = map[dedMoopType]string{
	dmDeductible: "Deductible",
	dmMOOP:       "MOOP",
}

func (ul dedMoopType) String() string {
	v, ok := dedMoopTypeValues[ul]
	if !ok {
		return "unknown"
	}
	return v
}

type medicalRxType int

const (
	_ medicalRxType = iota
	mrxMedical
	mrxDrug
	mrxCombined
)

var medicalRxTypeValues = map[medicalRxType]string{
	mrxMedical:  "Medical",
	mrxDrug:     "Drug",
	mrxCombined: "Combined",
}

func (ul medicalRxType) String() string {
	v, ok := medicalRxTypeValues[ul]
	if !ok {
		return "unknown"
	}
	return v
}

var deductibleTypeMap = map[medicalRxType]DeductibleType{
	mrxMedical:  MedicalDeductible,
	mrxDrug:     DrugDeductible,
	mrxCombined: CombinedDeductible,
}

var moopTypeMap = map[medicalRxType]MOOPType{
	mrxMedical:  MedicalMOOP,
	mrxDrug:     DrugMOOP,
	mrxCombined: CombinedMOOP,
}

type utilizationGroup struct {
	planLevel   bool
	personIndex int

	dedMoop   dedMoopType
	medicalRx medicalRxType

	applicableDeductible float64
	applicableMoop       float64
	remainingDeductible  float64
	remainingMoop        float64

	status oopcStatus

	nextApplicablePct float64
}

type oopcStatus int

const ( // numeric order is relevant
	oopcCSNoDed oopcStatus = iota
	oopcDed
	oopcCS
	oopcMOOP
)

func (hh *OopcHousehold) populateUtilizationGroups() error {
	// plan-level group
	for _, dedMoop := range [...]dedMoopType{dmDeductible, dmMOOP} {
		for _, medicalRx := range [...]medicalRxType{mrxMedical, mrxDrug, mrxCombined} {
			ug, err := hh.makeUtilizationGroup(true, 0, dedMoopType(dedMoop), medicalRxType(medicalRx))
			if err != nil {
				// TODO
			}
			if ug != nil {
				hh.utilGroups = append(hh.utilGroups, ug)
			}
		}
	}

	// group for each person
	for pi := range hh.People {
		for _, dedMoop := range [...]dedMoopType{dmDeductible, dmMOOP} {
			for _, medicalRx := range [...]medicalRxType{mrxMedical, mrxDrug, mrxCombined} {
				ug, err := hh.makeUtilizationGroup(false, pi, dedMoopType(dedMoop), medicalRxType(medicalRx))
				if err != nil {
					// TODO
				}
				if ug != nil {
					hh.utilGroups = append(hh.utilGroups, ug)
				}
			}
		}
	}

	return nil
}

// planLevel: this is either a plan-level group (the first) or for each person
// dedMoop: deductible or moop group
// medicalRx: medical, rx or combined group?
// return: you may receive a nil value, meaning this combination does not yield applicable deductible or moop
func (hh *OopcHousehold) makeUtilizationGroup(planLevel bool, personIndex int, dedMoop dedMoopType, medicalRx medicalRxType) (ug *utilizationGroup, err error) {
	var applies, ok bool
	var value float64

	var applicableDeductible, remainingDeductible float64
	var applicableMoop, remainingMoop float64

	// applies if plan-level && ( famsize == 1 && individual exists || famsize > 1 && fam group exists )
	// applies if person-level && ( individual exists )

	if dedMoop == dmDeductible {
		if planLevel {
			if len(hh.People) == 1 {
				value, ok = findDeductibleAmt(hh.plan, hh.variant, deductibleTypeMap[medicalRx], Individual)
			} else {
				value, ok = findDeductibleAmt(hh.plan, hh.variant, deductibleTypeMap[medicalRx], Family)
			}
		} else {
			value, ok = findDeductibleAmt(hh.plan, hh.variant, deductibleTypeMap[medicalRx], FamilyPerPerson)
		}

		applies = ok

		if ok {
			applicableDeductible = value
			remainingDeductible = value
		}
	}

	if dedMoop == dmMOOP {
		if planLevel {
			if len(hh.People) == 1 {
				value, ok = findMoopAmt(hh.plan, hh.variant, moopTypeMap[medicalRx], Individual)
			} else {
				value, ok = findMoopAmt(hh.plan, hh.variant, moopTypeMap[medicalRx], Family)
			}
		} else {
			value, ok = findMoopAmt(hh.plan, hh.variant, moopTypeMap[medicalRx], FamilyPerPerson)
		}

		applies = ok

		if ok {
			applicableMoop = value
			remainingMoop = value
		}
	}

	if !applies {
		return nil, nil
	}

	ug = &utilizationGroup{
		planLevel:            planLevel,
		personIndex:          personIndex,
		dedMoop:              dedMoop,
		medicalRx:            medicalRx,
		applicableDeductible: applicableDeductible,
		remainingDeductible:  remainingDeductible,
		applicableMoop:       applicableMoop,
		remainingMoop:        remainingMoop,
	}

	return
}

// favor InNetwork, then CombinedInOutNetwork, then does-not-exist
func findDeductibleAmt(plan *Plan, variant CSR, typ DeductibleType, familyCost FamilyCost) (float64, bool) {
	var inNetwork, combined float64
	var hasInNetwork, hasCombined bool

	for _, d := range plan.Deductibles {
		if d.Type == typ && d.CSR == variant && d.FamilyCost == familyCost {
			if d.NetworkTier == InNetwork {
				inNetwork = d.Amount
				hasInNetwork = true
			} else if d.NetworkTier == CombinedInOutNetwork {
				combined = d.Amount
				hasCombined = true
			}
		}
	}

	if hasInNetwork {
		return inNetwork, true
	} else if hasCombined {
		return combined, true
	} else {
		return 0, false
	}
}

// favor InNetwork, then CombinedInOutNetwork, then does-not-exist
func findMoopAmt(plan *Plan, variant CSR, typ MOOPType, familyCost FamilyCost) (float64, bool) {
	var inNetwork, combined float64
	var hasInNetwork, hasCombined bool

	for _, m := range plan.MOOPs {
		if m.Type == typ && m.CSR == variant && m.FamilyCost == familyCost {
			if m.NetworkTier == InNetwork {
				inNetwork = m.Amount
				hasInNetwork = true
			} else if m.NetworkTier == CombinedInOutNetwork {
				combined = m.Amount
				hasCombined = true
			}
		}
	}

	if hasInNetwork {
		return inNetwork, true
	} else if hasCombined {
		return combined, true
	} else {
		return 0, false
	}
}

func ageGroup(marketYear int, age int) string {
	switch {
	case age < 6:
		return "0-5"
	case age < 18:
		return "6-17"
	case age < 35:
		if marketYear < 2017 {
			return "18-34"
		} else {
			if age < 26 {
				return "18-25"
			} else {
				return "26-34"
			}
		}
	case age < 45:
		return "35-44"
	case age < 55:
		return "45-54"
	}
	return "55+"
}

func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Round away from zero.
func roundFloat64(x float64, prec int) float64 {
	power := 10.0
	for i := 1; i < prec; i++ {
		power *= 10
	}
	bump := 0.5
	if x < 0 {
		bump = -0.5
	}
	return float64(int(x*power+bump)) / power
}
