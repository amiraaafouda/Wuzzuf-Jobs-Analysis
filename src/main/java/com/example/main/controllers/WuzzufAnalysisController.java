package com.example.main.controllers;

import com.example.analysis.AnalysisHelper;
import com.example.analysis.Kmeans;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// import org.springframework.web;

@RestController
public class WuzzufAnalysisController {

    @GetMapping("/")
    public ResponseEntity<String> index() {
        String res = AnalysisHelper.getInstance().generateButtons();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/read")
    public ResponseEntity<String> readAndShow() {
        String res = AnalysisHelper.getInstance().readData();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/summary")
    public ResponseEntity<String> summary() {
        String res = AnalysisHelper.getInstance().getSummary();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/struct")
    public ResponseEntity<String> structure() {
        String res = AnalysisHelper.getInstance().getStructure();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/cleandata")
    public ResponseEntity<String> cleandata() {
        String res = AnalysisHelper.getInstance().cleanData();
        return ResponseEntity.ok(res);
    }

    // @GetMapping(value = "/most-popular-skills", produces = MediaType.IMAGE_JPEG_VALUE)
    // public @ResponseBody byte[] getImageWithMediaType() throws IOException {
    //     InputStream in = getClass().getResourceAsStream("/Most-Popular-Areas.png");
    //     // InputStream in = getClass().getResourceAsStream("/com/example/main/controllers/img.png");
    //     return IOUtils.toByteArray(in);
    // }
    @GetMapping("/count-jobs-table")
    public ResponseEntity<String> JobsPerCompany() {
        String html = AnalysisHelper.getInstance().getJobsTable();
        return ResponseEntity.ok().body(html);
    }

    @GetMapping("/most-jobs-chart")
    public ResponseEntity<String> mostJobsChart() {
        String html = AnalysisHelper.getInstance().getJobsChart();
        return ResponseEntity.ok(html);
    }


    @GetMapping("/most-skills-table")
    public ResponseEntity<String> mostPopularSkillsTable() {
        String html = AnalysisHelper.getInstance().getSkillsTable();

        return ResponseEntity.ok().body(html);
    }

    @GetMapping("/most-skills-chart")
    public ResponseEntity<String> mostPopularSkills() {

        String html = AnalysisHelper.getInstance().getSkillsChart();
        return ResponseEntity.ok().body(html);
    }


    @GetMapping("/most-titles-table")
    public ResponseEntity<String> mostPopularTitlesTable() {
        String html = AnalysisHelper.getInstance().getTitlesTable();
        return ResponseEntity.ok().body(html);
    }

    @GetMapping("/most-titles-chart")
    public ResponseEntity<String> mostPopularTitles() {
        String res = AnalysisHelper.getInstance().getTitlesChart();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/most-areas-table")
    public ResponseEntity<String> mostPopularAreasTable() {
        String html = AnalysisHelper.getInstance().getAreasTable();
        return ResponseEntity.ok().body(html);
    }


    @GetMapping("/most-areas-chart")
    public ResponseEntity<String> mostPopularAreas() {
        String res = AnalysisHelper.getInstance().getAreasChart();
        return ResponseEntity.ok(res);
    }


    @GetMapping("/k-means")
    public  ResponseEntity<String> K_means(){
        String html = AnalysisHelper.getInstance().getKmeans();
        return ResponseEntity.ok().body(html);
    }
}
