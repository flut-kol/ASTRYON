package defender

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
)

// This test doesn't work because of the following error:
//     chdir /loadlibrary: no such file or directory
// It occurs because the d.ScanFiles() function tries to perform os.Chdir("/loadlibrary") and we don't know how to fake it.
//
//func TestScanFiles(t *testing.T) {
//	d := makeDefaultTestDefender()
//
//	results, err := d.ScanFiles(context.Background(), []string{"1.doc"})
//
//	assert.Nil(t, err)
//	assert.Equal(t, 1, len(results))
//	assert.Equal(t, "1.doc", results[0].FileName)
//	assert.False(t, results[0].IsEncrypted)
//	assert.False(t, results[0].IsInfected)
//	assert.Empty(t, results[0].InfectionReason)
//	assert.Empty(t, results[0].Error)
//}

func TestParseFilesOutput_ThreeFiles_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()

	output := `main(): Scanning /files_for_scan/1.eml...
EngineScanCallback(): Scanning input
EngineScanCallback(): File input is identified as nUFS_html
main(): Scanning /files_for_scan/11.doc...
EngineScanCallback(): Scanning input
EngineScanCallback(): File input is identified as nUFS_ole2
EngineScanCallback(): File is encrypted.
main(): Scanning /files_for_scan/trj.pdf...
EngineScanCallback(): Scanning input
EngineScanCallback(): Scanning input->[pdfuri2rdata]
EngineScanCallback(): Packer nUFS_pdf identified.
EngineScanCallback(): Scanning input->(PdfUri:0000)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
EngineScanCallback(): Scanning input->(PdfUri:0001)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
EngineScanCallback(): Scanning input->(PdfUri:0002)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
`

	responses, err := d.parseFilesOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, 3, len(responses))

	assert.Equal(t, "1.eml", responses[0].FileName)
	assert.False(t, responses[0].IsEncrypted)
	assert.False(t, responses[0].IsInfected)
	assert.Empty(t, responses[0].InfectionReason)
	assert.Empty(t, responses[0].Error)

	assert.Equal(t, "11.doc", responses[1].FileName)
	assert.True(t, responses[1].IsEncrypted)
	assert.False(t, responses[1].IsInfected)
	assert.Empty(t, responses[1].InfectionReason)
	assert.Empty(t, responses[1].Error)

	assert.Equal(t, "trj.pdf", responses[2].FileName)
	assert.False(t, responses[2].IsEncrypted)
	assert.True(t, responses[2].IsInfected)
	assert.Equal(t, "Trojan:PDF/Phish", *responses[2].InfectionReason)
	assert.Empty(t, responses[2].Error)
}

func TestParseFileOutput_CleanFile_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()

	output := `main(): Scanning /files_for_scan/14.xlsx...
EngineScanCallback(): Scanning input
EngineScanCallback(): Packer nUFS_zip identified.
EngineScanCallback(): Scanning input->[Content_Types].xml
EngineScanCallback(): File input->[Content_Types].xml is identified as nUFS_html
EngineScanCallback(): Scanning input->_rels/.rels
EngineScanCallback(): File input->_rels/.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/_rels/workbook.xml.rels
EngineScanCallback(): File input->xl/_rels/workbook.xml.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/workbook.xml
EngineScanCallback(): File input->xl/workbook.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/sharedStrings.xml
EngineScanCallback(): File input->xl/sharedStrings.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/worksheets/_rels/sheet1.xml.rels
EngineScanCallback(): File input->xl/worksheets/_rels/sheet1.xml.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/theme/theme1.xml
EngineScanCallback(): File input->xl/theme/theme1.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/styles.xml
EngineScanCallback(): File input->xl/styles.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/worksheets/sheet1.xml
EngineScanCallback(): File input->xl/worksheets/sheet1.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/itemProps2.xml
EngineScanCallback(): File input->customXml/itemProps2.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->xl/printerSettings/printerSettings1.bin
EngineScanCallback(): Scanning input->customXml/_rels/item1.xml.rels
EngineScanCallback(): File input->customXml/_rels/item1.xml.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/_rels/item2.xml.rels
EngineScanCallback(): File input->customXml/_rels/item2.xml.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/_rels/item3.xml.rels
EngineScanCallback(): File input->customXml/_rels/item3.xml.rels is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/item1.xml
EngineScanCallback(): File input->customXml/item1.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/itemProps1.xml
EngineScanCallback(): File input->customXml/itemProps1.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->docProps/custom.xml
EngineScanCallback(): File input->docProps/custom.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/item3.xml
EngineScanCallback(): File input->customXml/item3.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/itemProps3.xml
EngineScanCallback(): File input->customXml/itemProps3.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->docProps/core.xml
EngineScanCallback(): File input->docProps/core.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->docProps/app.xml
EngineScanCallback(): File input->docProps/app.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->customXml/item2.xml
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "14.xlsx", r.FileName)
	assert.False(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}

func TestParseFileOutput_EncryptedFile_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()
	output := `main(): Scanning /files_for_scan/11.doc...
EngineScanCallback(): Scanning input
EngineScanCallback(): File input is identified as nUFS_ole2
EngineScanCallback(): File is encrypted.
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "11.doc", r.FileName)
	assert.True(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}

func TestParseFileOutput_FileContainerWhichIsEncrypted_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()
	output := `main(): Scanning /files_for_scan/67e.zip...
EngineScanCallback(): Scanning input
EngineScanCallback(): Packer nUFS_zip identified.
EngineScanCallback(): File is encrypted.
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "67e.zip", r.FileName)
	assert.True(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}

func TestParseFileOutput_FileContainerWhichHasNoEncryptedChildren_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()
	output := `main(): Scanning /files_for_scan/62d.zip...
EngineScanCallback(): File input->a6746296.xml is identified as nUFS_html
EngineScanCallback(): Scanning input->a6746296SGN.sgn
EngineScanCallback(): Scanning input->769/Протокол.pdf
EngineScanCallback(): Packer nUFS_pdf identified.
EngineScanCallback(): Scanning input->769/форма.pdf
EngineScanCallback(): Packer nUFS_pdf identified.`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "62d.zip", r.FileName)
	assert.False(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}

func TestParseFileOutput_FileContainerWhichHasEncryptedChildrenAsFirstFile_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()
	output := `main(): Scanning /files_for_scan/63f.zip...
EngineScanCallback(): Scanning input
EngineScanCallback(): Packer nUFS_zip identified.
EngineScanCallback(): Scanning input->46.xlsx
EngineScanCallback(): File input->46.xlsx is identified as nUFS_ole2
EngineScanCallback(): File is encrypted.
EngineScanCallback(): Scanning input->46_2022.xlsx
EngineScanCallback(): Packer nUFS_zip identified.
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "63f.zip", r.FileName)
	assert.False(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}

func TestParseFileOutput_FileContainerWhichHasEncryptedChildrenAsLastFile_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()
	output := `main(): Scanning /files_for_scan/63f.zip...
EngineScanCallback(): Scanning input
EngineScanCallback(): Packer nUFS_zip identified.
EngineScanCallback(): Scanning input->46_2022.xlsx
EngineScanCallback(): Packer nUFS_zip identified.
EngineScanCallback(): Scanning input->46.xlsx
EngineScanCallback(): File input->46.xlsx is identified as nUFS_ole2
EngineScanCallback(): File is encrypted.
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "63f.zip", r.FileName)
	assert.False(t, r.IsEncrypted)
	assert.False(t, r.IsInfected)
	assert.Empty(t, r.InfectionReason)
	assert.Empty(t, r.Error)
}
func TestParseFileOutput_InfectedFile_DetectedCorrectly(t *testing.T) {
	d := makeDefaultTestDefender()

	output := `main(): Scanning /files_for_scan/trj.pdf...
EngineScanCallback(): Scanning input
EngineScanCallback(): Scanning input->[pdfuri2rdata]
EngineScanCallback(): Packer nUFS_pdf identified.
EngineScanCallback(): Scanning input->(PdfUri:0000)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
EngineScanCallback(): Scanning input->(PdfUri:0001)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
EngineScanCallback(): Scanning input->(PdfUri:0002)
EngineScanCallback(): Threat Trojan:PDF/Phish identified.
`

	r, err := d.parseFileOutput(output)

	assert.Nil(t, err)
	assert.Equal(t, "trj.pdf", r.FileName)
	assert.False(t, r.IsEncrypted)
	assert.True(t, r.IsInfected)
	assert.Equal(t, "Trojan:PDF/Phish", *r.InfectionReason)
	assert.Empty(t, r.Error)
}

func makeDefaultTestDefender() *defender {
	return &defender{
		timeout:      int64(60),
		sharedFolder: "/files_for_scan",
		cmd:          fakeMpClientClean,
	}
}

func fakeMpClientClean(ctx context.Context, command string, args ...string) *exec.Cmd {
	c := []string{"-test.run=TestMpClient_Clean", "--", command}
	c = append(c, args...)

	cmd := exec.CommandContext(ctx, os.Args[0], c...)
	cmd.Env = []string{"GO_TEST_PROCESS=1"}
	return cmd
}

func TestMpClient_Clean(t *testing.T) {
	if os.Getenv("GO_TEST_PROCESS") != "1" {
		return
	}

	fmt.Print(`main(): Scanning /files_for_scan/1.doc...
EngineScanCallback(): Scanning input
`)
	os.Exit(0)
}

func makeTestDefender(cmdContext commandContext) *defender {
	return &defender{
		timeout:      int64(60),
		sharedFolder: "/files_for_scan",
		cmd:          cmdContext,
	}
}

func TestMpClient_IsEncrypted(t *testing.T) {
	if os.Getenv("GO_TEST_PROCESS") != "1" {
		return
	}

	fmt.Print(`main(): Scanning /files_for_scan/11.doc...
EngineScanCallback(): Scanning input
EngineScanCallback(): File input is identified as nUFS_ole2
EngineScanCallback(): File is encrypted.
`)
	os.Exit(0)
}

func fakeMpClientIsEncrypted(ctx context.Context, command string, args ...string) *exec.Cmd {
	c := []string{"-test.run=TestMpClient_IsEncrypted", "--", command}
	c = append(c, args...)

	cmd := exec.CommandContext(ctx, os.Args[0], c...)
	cmd.Env = []string{"GO_TEST_PROCESS=1"}
	return cmd
}
